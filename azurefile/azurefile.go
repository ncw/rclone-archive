// Package azurefile provides an interface to the Microsoft Azure file storage system
package azurefile

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"net/http"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/storage"
	"github.com/ncw/rclone/fs"
	"github.com/ncw/rclone/pacer"
	"github.com/pkg/errors"
)

const (
	apiVersion    = "2017-04-17"
	minSleep      = 10 * time.Millisecond
	maxSleep      = 10 * time.Second
	decayConstant = 1    // bigger for slower decay, exponential
	listChunkSize = 5000 // number of items to read at once
	modTimeKey    = "mtime"
	timeFormatIn  = time.RFC3339
	timeFormatOut = "2006-01-02T15:04:05.000000000Z07:00"
)

// Globals
var (
	maxChunkSize    = fs.SizeSuffix(100 * 1024 * 1024)
	chunkSize       = fs.SizeSuffix(4 * 1024 * 1024)
	uploadCutoff    = fs.SizeSuffix(256 * 1024 * 1024)
	maxUploadCutoff = fs.SizeSuffix(256 * 1024 * 1024)
)

// Register with Fs
func init() {
	fs.Register(&fs.RegInfo{
		Name:        "azurefile",
		Description: "Microsoft Azure File Storage",
		NewFs:       NewFs,
		Options: []fs.Option{{
			Name: "account",
			Help: "Storage Account Name",
		}, {
			Name: "key",
			Help: "Storage Account Key",
		}, {
			Name: "share",
			Help: "File share name",
		}, {
			Name: "endpoint",
			Help: "Endpoint for the service - leave blank normally.",
		},
		},
	})
	fs.VarP(&uploadCutoff, "azurefile-upload-cutoff", "", "Cutoff for switching to chunked upload")
	fs.VarP(&chunkSize, "azurefile-chunk-size", "", "Upload chunk size. Must fit in memory.")
}

// Fs represents a remote azure server
type Fs struct {
	name             string       // name of this remote
	root             string       // the path we are working on if any
	features         *fs.Features // optional features
	account          string       // account name
	key              []byte       // auth key
	endpoint         string       // name of the starting api endpoint
	srv              *storage.Share
	container        string                // the container we are working on
	containerOKMu    sync.Mutex            // mutex to protect container OK
	containerOK      bool                  // true if we have created the container
	containerDeleted bool                  // true if we have deleted the container
	pacer            *pacer.Pacer          // To pace and retry the API calls
	uploadToken      *pacer.TokenDispenser // control concurrency
}

// Object describes a azure object
type Object struct {
	fs       *Fs               // what this object is part of
	remote   string            // The remote path
	id       string            // azure id of the file
	modTime  time.Time         // The modified time of the object if known
	md5      string            // SHA-1 hash if known
	size     int64             // Size of the object
	mimeType string            // Content-Type of the object
	meta     map[string]string // file metadata
}

// ------------------------------------------------------------

// Name of the remote (as passed into NewFs)
func (f *Fs) Name() string {
	return f.name
}

// Root of the remote (as passed into NewFs)
func (f *Fs) Root() string {
	if f.root == "" {
		return f.container
	}
	return f.container + "/" + f.root
}

// String converts this Fs to a string
func (f *Fs) String() string {
	if f.root == "" {
		return fmt.Sprintf("Azure container %s", f.container)
	}
	return fmt.Sprintf("Azure container %s path %s", f.container, f.root)
}

// Features returns the optional features of this Fs
func (f *Fs) Features() *fs.Features {
	return f.features
}

// Pattern to match a azure path
var matcher = regexp.MustCompile(`^([^/]*)(.*)$`)

// parseParse parses a azure 'url'
func parsePath(path string) (container, directory string, err error) {
	parts := matcher.FindStringSubmatch(path)
	if parts == nil {
		err = errors.Errorf("couldn't find container in azure path %q", path)
	} else {
		container, directory = parts[1], parts[2]
		directory = strings.Trim(directory, "/")
	}
	return
}

// retryErrorCodes is a slice of error codes that we will retry
var retryErrorCodes = []int{
	401, // Unauthorized (eg "Token has expired")
	408, // Request Timeout
	429, // Rate exceeded.
	500, // Get occasional 500 Internal Server Error
	503, // Service Unavailable
	504, // Gateway Time-out
}

// shouldRetry returns a boolean as to whether this resp and err
// deserve to be retried.  It returns the err as a convenience
func (f *Fs) shouldRetry(err error) (bool, error) {
	// FIXME interpret special errors - more to do here
	if storageErr, ok := err.(storage.AzureStorageServiceError); ok {
		statusCode := storageErr.StatusCode
		for _, e := range retryErrorCodes {
			if statusCode == e {
				return true, err
			}
		}
	}
	return fs.ShouldRetry(err), err
}

// NewFs contstructs an Fs from the path, container:path
func NewFs(name, root string) (fs.Fs, error) {
	if uploadCutoff > maxUploadCutoff {
		return nil, errors.Errorf("azure: upload cutoff (%v) must be less than or equal to %v", uploadCutoff, maxUploadCutoff)
	}
	if chunkSize > maxChunkSize {
		return nil, errors.Errorf("azure: chunk size can't be greater than %v - was %v", maxChunkSize, chunkSize)
	}
	container, directory, err := parsePath(root)
	if err != nil {
		return nil, err
	}
	account := fs.ConfigFileGet(name, "account")
	if account == "" {
		return nil, errors.New("account not found")
	}
	share := fs.ConfigFileGet(name, "share")
	if share == "" {
		return nil, errors.New("share not found")
	}
	key := fs.ConfigFileGet(name, "key")
	if key == "" {
		return nil, errors.New("key not found")
	}
	keyBytes, err := base64.StdEncoding.DecodeString(key)
	if err != nil {
		return nil, errors.Errorf("malformed storage account key: %v", err)
	}

	endpoint := fs.ConfigFileGet(name, "endpoint", storage.DefaultBaseURL)

	client, err := storage.NewClient(account, key, endpoint, apiVersion, true)
	if err != nil {
		return nil, errors.Wrap(err, "failed to make azure storage client")
	}
	client.HTTPClient = fs.Config.Client()
	files := client.GetFileService()
	srv := files.GetShareReference(share)

	f := &Fs{
		name:        name,
		container:   container,
		root:        directory,
		account:     account,
		key:         keyBytes,
		endpoint:    endpoint,
		srv:         srv,
		pacer:       pacer.New().SetMinSleep(minSleep).SetMaxSleep(maxSleep).SetDecayConstant(decayConstant),
		uploadToken: pacer.NewTokenDispenser(fs.Config.Transfers),
	}
	f.features = (&fs.Features{ /*ReadMimeType: true , FIXME WriteMimeType: true*/ }).Fill(f)
	if f.root != "" {
		f.root += "/"
		// Check to see if the (container,directory) is actually an existing file
		oldRoot := f.root
		remote := path.Base(directory)
		f.root = path.Dir(directory)
		if f.root == "." {
			f.root = ""
		} else {
			f.root += "/"
		}
		_, err := f.NewObject(remote)
		if err != nil {
			if err == fs.ErrorObjectNotFound {
				// File doesn't exist so return old f
				f.root = oldRoot
				return f, nil
			}
			return nil, err
		}
		// return an error with an fs which points to the parent
		return f, fs.ErrorIsFile
	}
	return f, nil
}

// Return an Object from a path
//
// If it can't be found it returns the error fs.ErrorObjectNotFound.
func (f *Fs) newObjectWithInfo(remote string, info *storage.File) (fs.Object, error) {
	o := &Object{
		fs:     f,
		remote: remote,
	}
	if info != nil {
		err := o.decodeMetaData(info)
		if err != nil {
			return nil, err
		}
	} else {
		err := o.readMetaData() // reads info and headers, returning an error
		if err != nil {
			return nil, err
		}
	}
	return o, nil
}

// NewObject finds the Object at remote.  If it can't be found
// it returns the error fs.ErrorObjectNotFound.
func (f *Fs) NewObject(remote string) (fs.Object, error) {
	return f.newObjectWithInfo(remote, nil)
}

// getFileReference creates an empty file reference with no metadata
func (f *Fs) getFileReference(remote string) *storage.File {
	return f.srv.GetFileReference(f.root + remote)
}

// getFileWithModTime adds the modTime passed in to o.meta and creates
// a File from it.
func (o *Object) getFileWithModTime(modTime time.Time) *storage.File {
	// Make sure o.meta is not nil
	if o.meta == nil {
		o.meta = make(map[string]string, 1)
	}

	// Set modTimeKey in it
	o.meta[modTimeKey] = modTime.Format(timeFormatOut)

	file := o.getFileReference()
	file.Metadata = o.meta
	return file
}

// listFn is called from list to handle an object
type listFn func(remote string, object *storage.File, isDirectory bool) error

// list lists the objects into the function supplied from
// the container and root supplied
//
// dir is the starting directory, "" for root
func (f *Fs) list(dir string, recurse bool, maxResults uint, fn listFn) error {
	f.containerOKMu.Lock()
	deleted := f.containerDeleted
	f.containerOKMu.Unlock()
	if deleted {
		return fs.ErrorDirNotFound
	}
	root := f.root
	if dir != "" {
		root += dir + "/"
	}
	delimiter := ""
	if !recurse {
		delimiter = "/"
	}
	params := storage.ListFilesParameters{
		MaxResults: maxResults,
		Prefix:     root,
		Delimiter:  delimiter,
		Include: &storage.IncludeFileDataset{
			Snapshots:        false,
			Metadata:         true,
			UncommittedFiles: false,
			Copy:             false,
		},
	}
	for {
		var response storage.FileListResponse
		err := f.pacer.Call(func() (bool, error) {
			var err error
			response, err = f.srv.ListFiles(params)
			return f.shouldRetry(err)
		})
		if err != nil {
			if storageErr, ok := err.(storage.AzureStorageServiceError); ok && storageErr.StatusCode == http.StatusNotFound {
				return fs.ErrorDirNotFound
			}
			return err
		}
		for i := range response.Files {
			file := &response.Files[i]
			// Finish if file name no longer has prefix
			// if prefix != "" && !strings.HasPrefix(file.Name, prefix) {
			// 	return nil
			// }
			if !strings.HasPrefix(file.Name, f.root) {
				fs.Debugf(f, "Odd name received %q", file.Name)
				continue
			}
			remote := file.Name[len(f.root):]
			// Check for directory
			isDirectory := strings.HasSuffix(remote, "/")
			if isDirectory {
				remote = remote[:len(remote)-1]
			}
			// Send object
			err = fn(remote, file, isDirectory)
			if err != nil {
				return err
			}
		}
		// Send the subdirectories
		for _, remote := range response.FilePrefixes {
			remote := strings.TrimRight(remote, "/")
			// Send object
			err = fn(remote, nil, true)
			if err != nil {
				return err
			}
		}
		// end if no NextFileName
		if response.NextMarker == "" {
			break
		}
		params.Marker = response.NextMarker
	}
	return nil
}

// Convert a list item into a DirEntry
func (f *Fs) itemToDirEntry(remote string, object *storage.File, isDirectory bool) (fs.DirEntry, error) {
	if isDirectory {
		d := fs.NewDir(remote, time.Time{})
		return d, nil
	}
	o, err := f.newObjectWithInfo(remote, object)
	if err != nil {
		return nil, err
	}
	return o, nil
}

// listDir lists a single directory
func (f *Fs) listDir(dir string) (entries fs.DirEntries, err error) {
	err = f.list(dir, false, listChunkSize, func(remote string, object *storage.File, isDirectory bool) error {
		entry, err := f.itemToDirEntry(remote, object, isDirectory)
		if err != nil {
			return err
		}
		if entry != nil {
			entries = append(entries, entry)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return entries, nil
}

// listContainers returns all the containers to out
func (f *Fs) listContainers(dir string) (entries fs.DirEntries, err error) {
	if dir != "" {
		return nil, fs.ErrorListBucketRequired
	}
	err = f.listContainersToFn(func(container *storage.Container) error {
		d := fs.NewDir(container.Name, time.Time{})
		entries = append(entries, d)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return entries, nil
}

// List the objects and directories in dir into entries.  The
// entries can be returned in any order but should be for a
// complete directory.
//
// dir should be "" to list the root, and should not have
// trailing slashes.
//
// This should return ErrDirNotFound if the directory isn't
// found.
func (f *Fs) List(dir string) (entries fs.DirEntries, err error) {
	if f.container == "" {
		return f.listContainers(dir)
	}
	return f.listDir(dir)
}

// ListR lists the objects and directories of the Fs starting
// from dir recursively into out.
//
// dir should be "" to start from the root, and should not
// have trailing slashes.
//
// This should return ErrDirNotFound if the directory isn't
// found.
//
// It should call callback for each tranche of entries read.
// These need not be returned in any particular order.  If
// callback returns an error then the listing will stop
// immediately.
//
// Don't implement this unless you have a more efficient way
// of listing recursively that doing a directory traversal.
func (f *Fs) ListR(dir string, callback fs.ListRCallback) (err error) {
	if f.container == "" {
		return fs.ErrorListBucketRequired
	}
	list := fs.NewListRHelper(callback)
	err = f.list(dir, true, listChunkSize, func(remote string, object *storage.File, isDirectory bool) error {
		entry, err := f.itemToDirEntry(remote, object, isDirectory)
		if err != nil {
			return err
		}
		return list.Add(entry)
	})
	if err != nil {
		return err
	}
	return list.Flush()
}

// listContainerFn is called from listContainersToFn to handle a container
type listContainerFn func(*storage.Container) error

// listContainersToFn lists the containers to the function supplied
func (f *Fs) listContainersToFn(fn listContainerFn) error {
	// FIXME page the containers if necessary?
	params := storage.ListContainersParameters{}
	var response *storage.ContainerListResponse
	err := f.pacer.Call(func() (bool, error) {
		var err error
		response, err = f.srv.ListContainers(params)
		return f.shouldRetry(err)
	})
	if err != nil {
		return err
	}
	for i := range response.Containers {
		err = fn(&response.Containers[i])
		if err != nil {
			return err
		}
	}
	return nil
}

// Put the object into the container
//
// Copy the reader in to the new object which is returned
//
// The new object may have been created if an error is returned
func (f *Fs) Put(in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	// Temporary Object under construction
	fs := &Object{
		fs:     f,
		remote: src.Remote(),
	}
	return fs, fs.Update(in, src, options...)
}

// Mkdir creates the container if it doesn't exist
func (f *Fs) Mkdir(dir string) error {
	f.containerOKMu.Lock()
	defer f.containerOKMu.Unlock()
	if f.containerOK {
		return nil
	}
	options := storage.CreateContainerOptions{
		Access: storage.ContainerAccessTypePrivate,
	}
	err := f.pacer.Call(func() (bool, error) {
		err := f.srv.Create(&options)
		if err != nil {
			if storageErr, ok := err.(storage.AzureStorageServiceError); ok {
				switch storageErr.StatusCode {
				case http.StatusConflict:
					switch storageErr.Code {
					case "ContainerAlreadyExists":
						f.containerOK = true
						return false, nil
					case "ContainerBeingDeleted":
						f.containerDeleted = true
						return true, err
					}
				}
			}
		}
		return f.shouldRetry(err)
	})
	if err == nil {
		f.containerOK = true
		f.containerDeleted = false
	}
	return errors.Wrap(err, "failed to make container")
}

// isEmpty checks to see if a given directory is empty and returns an error if not
func (f *Fs) isEmpty(dir string) (err error) {
	empty := true
	err = f.list("", true, 1, func(remote string, object *storage.File, isDirectory bool) error {
		empty = false
		return nil
	})
	if err != nil {
		return err
	}
	if !empty {
		return fs.ErrorDirectoryNotEmpty
	}
	return nil
}

// deleteContainer deletes the container.  It can delete a full
// container so use isEmpty if you don't want that.
func (f *Fs) deleteContainer() error {
	f.containerOKMu.Lock()
	defer f.containerOKMu.Unlock()
	options := storage.DeleteContainerOptions{}
	err := f.pacer.Call(func() (bool, error) {
		exists, err := f.srv.Exists()
		if err != nil {
			return f.shouldRetry(err)
		}
		if !exists {
			return false, fs.ErrorDirNotFound
		}
		err = f.srv.Delete(&options)
		return f.shouldRetry(err)
	})
	if err == nil {
		f.containerOK = false
		f.containerDeleted = true
	}
	return errors.Wrap(err, "failed to delete container")
}

// Rmdir deletes the container if the fs is at the root
//
// Returns an error if it isn't empty
func (f *Fs) Rmdir(dir string) error {
	err := f.isEmpty(dir)
	if err != nil {
		return err
	}
	if f.root != "" || dir != "" {
		return nil
	}
	return f.deleteContainer()
}

// Precision of the remote
func (f *Fs) Precision() time.Duration {
	return time.Nanosecond
}

// Hashes returns the supported hash sets.
func (f *Fs) Hashes() fs.HashSet {
	return fs.HashSet(fs.HashMD5)
}

// Purge deletes all the files and directories including the old versions.
func (f *Fs) Purge() error {
	dir := "" // FIXME forward compat!
	if f.root != "" || dir != "" {
		// Delegate to caller if not root container
		return fs.ErrorCantPurge
	}
	return f.deleteContainer()
}

// Copy src to this remote using server side copy operations.
//
// This is stored with the remote path given
//
// It returns the destination Object and a possible error
//
// Will only be called if src.Fs().Name() == f.Name()
//
// If it isn't possible then return fs.ErrorCantCopy
func (f *Fs) Copy(src fs.Object, remote string) (fs.Object, error) {
	err := f.Mkdir("")
	if err != nil {
		return nil, err
	}
	srcObj, ok := src.(*Object)
	if !ok {
		fs.Debugf(src, "Can't copy - not same remote type")
		return nil, fs.ErrorCantCopy
	}
	dstFile := f.getFileReference(remote)
	srcFile := srcObj.getFileReference()
	options := storage.CopyOptions{}
	sourceFileURL := srcFile.GetURL()
	err = f.pacer.Call(func() (bool, error) {
		err = dstFile.Copy(sourceFileURL, &options)
		return f.shouldRetry(err)
	})
	if err != nil {
		return nil, err
	}
	return f.NewObject(remote)
}

// ------------------------------------------------------------

// Fs returns the parent Fs
func (o *Object) Fs() fs.Info {
	return o.fs
}

// Return a string version
func (o *Object) String() string {
	if o == nil {
		return "<nil>"
	}
	return o.remote
}

// Remote returns the remote path
func (o *Object) Remote() string {
	return o.remote
}

// Hash returns the MD5 of an object returning a lowercase hex string
func (o *Object) Hash(t fs.HashType) (string, error) {
	if t != fs.HashMD5 {
		return "", fs.ErrHashUnsupported
	}
	// Convert base64 encoded md5 into lower case hex
	if o.md5 == "" {
		return "", nil
	}
	data, err := base64.StdEncoding.DecodeString(o.md5)
	if err != nil {
		return "", errors.Wrapf(err, "Failed to decode Content-MD5: %q", o.md5)
	}
	return hex.EncodeToString(data), nil
}

// Size returns the size of an object in bytes
func (o *Object) Size() int64 {
	return o.size
}

// decodeMetaData sets the metadata from the data passed in
//
// Sets
//  o.id
//  o.modTime
//  o.size
//  o.md5
//  o.meta
func (o *Object) decodeMetaData(info *storage.File) (err error) {
	o.md5 = info.Properties.ContentMD5
	o.mimeType = info.Properties.ContentType
	o.size = info.Properties.ContentLength
	o.modTime = time.Time(info.Properties.LastModified)
	if len(info.Metadata) > 0 {
		o.meta = info.Metadata
		if modTime, ok := info.Metadata[modTimeKey]; ok {
			when, err := time.Parse(timeFormatIn, modTime)
			if err != nil {
				fs.Debugf(o, "Couldn't parse %v = %q: %v", modTimeKey, modTime, err)
			}
			o.modTime = when
		}
	} else {
		o.meta = nil
	}
	return nil
}

// getFileReference creates an empty file reference with no metadata
func (o *Object) getFileReference() *storage.File {
	return o.fs.getFileReference(o.remote)
}

// clearMetaData clears enough metadata so readMetaData will re-read it
func (o *Object) clearMetaData() {
	o.modTime = time.Time{}
}

// readMetaData gets the metadata if it hasn't already been fetched
//
// Sets
//  o.id
//  o.modTime
//  o.size
//  o.md5
func (o *Object) readMetaData() (err error) {
	if !o.modTime.IsZero() {
		return nil
	}
	file := o.getFileReference()

	// Read metadata (this includes metadata)
	getPropertiesOptions := storage.GetFilePropertiesOptions{}
	err = o.fs.pacer.Call(func() (bool, error) {
		err = file.GetProperties(&getPropertiesOptions)
		return o.fs.shouldRetry(err)
	})
	if err != nil {
		if storageErr, ok := err.(storage.AzureStorageServiceError); ok && storageErr.StatusCode == http.StatusNotFound {
			return fs.ErrorObjectNotFound
		}
		return err
	}

	return o.decodeMetaData(file)
}

// timeString returns modTime as the number of milliseconds
// elapsed since January 1, 1970 UTC as a decimal string.
func timeString(modTime time.Time) string {
	return strconv.FormatInt(modTime.UnixNano()/1E6, 10)
}

// parseTimeString converts a decimal string number of milliseconds
// elapsed since January 1, 1970 UTC into a time.Time and stores it in
// the modTime variable.
func (o *Object) parseTimeString(timeString string) (err error) {
	if timeString == "" {
		return nil
	}
	unixMilliseconds, err := strconv.ParseInt(timeString, 10, 64)
	if err != nil {
		fs.Debugf(o, "Failed to parse mod time string %q: %v", timeString, err)
		return err
	}
	o.modTime = time.Unix(unixMilliseconds/1E3, (unixMilliseconds%1E3)*1E6).UTC()
	return nil
}

// ModTime returns the modification time of the object
//
// It attempts to read the objects mtime and if that isn't present the
// LastModified returned in the http headers
//
// SHA-1 will also be updated once the request has completed.
func (o *Object) ModTime() (result time.Time) {
	// The error is logged in readMetaData
	_ = o.readMetaData()
	return o.modTime
}

// SetModTime sets the modification time of the local fs object
func (o *Object) SetModTime(modTime time.Time) error {
	file := o.getFileWithModTime(modTime)
	options := storage.SetFileMetadataOptions{}
	err := o.fs.pacer.Call(func() (bool, error) {
		err := file.SetMetadata(&options)
		return o.fs.shouldRetry(err)
	})
	if err != nil {
		return err
	}
	o.modTime = modTime
	return nil
}

// Storable returns if this object is storable
func (o *Object) Storable() bool {
	return true
}

// openFile represents an Object open for reading
type openFile struct {
	o     *Object        // Object we are reading for
	resp  *http.Response // response of the GET
	body  io.Reader      // reading from here
	hash  hash.Hash      // currently accumulating MD5
	bytes int64          // number of bytes read on this connection
	eof   bool           // whether we have read end of file
}

// Open an object for read
func (o *Object) Open(options ...fs.OpenOption) (in io.ReadCloser, err error) {
	getFileOptions := storage.GetFileOptions{}
	getFileRangeOptions := storage.GetFileRangeOptions{
		GetFileOptions: &getFileOptions,
	}
	for _, option := range options {
		switch x := option.(type) {
		case *fs.RangeOption:
			getFileRangeOptions.Range = &storage.FileRange{
				Start: uint64(x.Start),
				End:   uint64(x.End),
			}
		case *fs.SeekOption:
			getFileRangeOptions.Range = &storage.FileRange{
				Start: uint64(x.Offset),
				End:   uint64(o.size),
			}
		default:
			if option.Mandatory() {
				fs.Logf(o, "Unsupported mandatory option: %v", option)
			}
		}
	}
	file := o.getFileReference()
	err = o.fs.pacer.Call(func() (bool, error) {
		if getFileRangeOptions.Range == nil {
			in, err = file.Get(&getFileOptions)
		} else {
			in, err = file.GetRange(&getFileRangeOptions)
		}
		return o.fs.shouldRetry(err)
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to open for download")
	}
	return in, nil
}

// dontEncode is the characters that do not need percent-encoding
//
// The characters that do not need percent-encoding are a subset of
// the printable ASCII characters: upper-case letters, lower-case
// letters, digits, ".", "_", "-", "/", "~", "!", "$", "'", "(", ")",
// "*", ";", "=", ":", and "@". All other byte values in a UTF-8 must
// be replaced with "%" and the two-digit hex value of the byte.
const dontEncode = (`abcdefghijklmnopqrstuvwxyz` +
	`ABCDEFGHIJKLMNOPQRSTUVWXYZ` +
	`0123456789` +
	`._-/~!$'()*;=:@`)

// noNeedToEncode is a bitmap of characters which don't need % encoding
var noNeedToEncode [256]bool

func init() {
	for _, c := range dontEncode {
		noNeedToEncode[c] = true
	}
}

// urlEncode encodes in with % encoding
func urlEncode(in string) string {
	var out bytes.Buffer
	for i := 0; i < len(in); i++ {
		c := in[i]
		if noNeedToEncode[c] {
			_ = out.WriteByte(c)
		} else {
			_, _ = out.WriteString(fmt.Sprintf("%%%2X", c))
		}
	}
	return out.String()
}

// uploadMultipart uploads a file using multipart upload
//
// Write a larger file, using CreateBlockFile, PutBlock, and PutBlockList.
func (o *Object) uploadMultipart(in io.Reader, size int64, file *storage.File, putFileOptions *storage.PutFileOptions) (err error) {
	// First create an empty file
	err = o.fs.pacer.Call(func() (bool, error) {
		err := file.CreateBlockFile(putFileOptions)
		return o.fs.shouldRetry(err)
	})

	// Calculate number of parts
	totalParts, remainder := size/int64(chunkSize), size%int64(chunkSize)
	if remainder != 0 {
		totalParts++
	}
	fs.Debugf(o, "Multipart upload session started for %d parts of size %v", totalParts, fs.SizeSuffix(chunkSize))

	// block ID variables
	var (
		rawID   uint64
		bytesID = make([]byte, 8)
		blockID = "" // id in base64 encoded form
		blocks  = make([]storage.Block, 0, totalParts)
	)

	// increment the blockID
	nextID := func() {
		rawID++
		binary.LittleEndian.PutUint64(bytesID, rawID)
		blockID = base64.StdEncoding.EncodeToString(bytesID)
		blocks = append(blocks, storage.Block{
			ID:     blockID,
			Status: storage.BlockStatusLatest,
		})
	}

	// Upload the chunks
	remaining := size
	position := int64(0)
	errs := make(chan error, 1)
	var wg sync.WaitGroup
outer:
	for part := 0; part < int(totalParts); part++ {
		// Check any errors
		select {
		case err = <-errs:
			break outer
		default:
		}

		reqSize := remaining
		if reqSize >= int64(chunkSize) {
			reqSize = int64(chunkSize)
		}

		// Make a block of memory
		buf := make([]byte, reqSize)

		// Read the chunk
		_, err = io.ReadFull(in, buf)
		if err != nil {
			err = errors.Wrap(err, "multipart upload failed to read source")
			break outer
		}

		// Transfer the chunk
		nextID()
		wg.Add(1)
		o.fs.uploadToken.Get()
		go func(part int, position int64, blockID string) {
			defer wg.Done()
			defer o.fs.uploadToken.Put()
			fs.Debugf(o, "Uploading part %d/%d offset %v/%v part size %v", part+1, totalParts, fs.SizeSuffix(position), fs.SizeSuffix(size), fs.SizeSuffix(chunkSize))

			// Upload the block, with MD5 for check
			md5sum := md5.Sum(buf)
			putBlockOptions := storage.PutBlockOptions{
				ContentMD5: base64.StdEncoding.EncodeToString(md5sum[:]),
			}
			err = o.fs.pacer.Call(func() (bool, error) {
				err = file.PutBlockWithLength(blockID, uint64(len(buf)), bytes.NewBuffer(buf), &putBlockOptions)
				return o.fs.shouldRetry(err)
			})

			if err != nil {
				err = errors.Wrap(err, "multipart upload failed to upload part")
				select {
				case errs <- err:
				default:
				}
				return
			}
		}(part, position, blockID)

		// ready for next block
		remaining -= int64(chunkSize)
		position += int64(chunkSize)
	}
	wg.Wait()
	if err == nil {
		select {
		case err = <-errs:
		default:
		}
	}
	if err != nil {
		return err
	}

	// Finalise the upload session
	putBlockListOptions := storage.PutBlockListOptions{}
	err = o.fs.pacer.Call(func() (bool, error) {
		err := file.PutBlockList(blocks, &putBlockListOptions)
		return o.fs.shouldRetry(err)
	})
	if err != nil {
		return errors.Wrap(err, "multipart upload failed to finalize")
	}
	return nil
}

// Update the object with the contents of the io.Reader, modTime and size
//
// The new object may have been created if an error is returned
func (o *Object) Update(in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (err error) {
	err = o.fs.Mkdir("")
	if err != nil {
		return err
	}
	size := src.Size()
	file := o.getFileWithModTime(src.ModTime())
	putFileOptions := storage.PutFileOptions{
	// FIXME ContentType: fs.MimeType(o),
	}
	// FIXME not until https://github.com/Azure/azure-sdk-for-go/pull/702 is complete
	// and even then it didn't seem to work for block files?
	// if sourceMD5, _ := src.Hash(fs.HashSHA1); sourceMD5 != "" {
	// 	putFileOptions.ContentMD5 = base64.StdEncoding.EncodeToString([]byte(sourceMD5))
	// }

	// Don't retry, return a retry error instead
	err = o.fs.pacer.CallNoRetry(func() (bool, error) {
		if size >= int64(uploadCutoff) {
			// If a large file upload in chunks
			err = o.uploadMultipart(in, size, file, &putFileOptions)
		} else {
			// Write a small file in one transaction
			err = file.CreateBlockFileFromReader(in, &putFileOptions)
		}
		return o.fs.shouldRetry(err)
	})
	if err != nil {
		return err
	}
	o.clearMetaData()
	return o.readMetaData()
}

// Remove an object
func (o *Object) Remove() error {
	file := o.getFileReference()
	options := storage.DeleteFileOptions{}
	return o.fs.pacer.Call(func() (bool, error) {
		err := file.Delete(&options)
		return o.fs.shouldRetry(err)
	})
}

// MimeType of an Object if known, "" otherwise
func (o *Object) FIXMEMimeType() string {
	return o.mimeType
}

// Check the interfaces are satisfied
var (
	_ fs.Fs      = &Fs{}
	_ fs.Copier  = &Fs{}
	_ fs.Purger  = &Fs{}
	_ fs.ListRer = &Fs{}
	_ fs.Object  = &Object{}
	// FIXME _ fs.MimeTyper = &Object{}
)
