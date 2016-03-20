// Package amazonclouddrive provides an interface to the Amazon Cloud
// Drive object storage system.
package amazonclouddrive

/*

FIXME make searching for directory in id and file in id more efficient
- use the name: search parameter - remember the escaping rules
- use Folder GetNode and GetFile

FIXME make the default for no files and no dirs be (FILE & FOLDER) so
we ignore assets completely!
*/

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/ncw/go-acd"
	"github.com/ncw/rclone/dircache"
	"github.com/ncw/rclone/fs"
	"github.com/ncw/rclone/oauthutil"
	"github.com/ncw/rclone/pacer"
	"github.com/spf13/pflag"
	"golang.org/x/oauth2"
)

const (
	rcloneClientID              = "amzn1.application-oa2-client.6bf18d2d1f5b485c94c8988bb03ad0e7"
	rcloneEncryptedClientSecret = "k8/NyszKm5vEkZXAwsbGkd6C3NrbjIqMg4qEhIeF14Szub2wur+/teS3ubXgsLe9//+tr/qoqK+lq6mg8vWkoA=="
	folderKind                  = "FOLDER"
	fileKind                    = "FILE"
	assetKind                   = "ASSET"
	statusAvailable             = "AVAILABLE"
	timeFormat                  = time.RFC3339 // 2014-03-07T22:31:12.173Z
	minSleep                    = 20 * time.Millisecond
	warnFileSize                = 50 << 30 // Display warning for files larger than this size
)

// Globals
var (
	// Flags
	tempLinkThreshold = fs.SizeSuffix(9 << 30) // Download files bigger than this via the tempLink
	// Description of how to auth for this app
	acdConfig = &oauth2.Config{
		Scopes: []string{"clouddrive:read_all", "clouddrive:write"},
		Endpoint: oauth2.Endpoint{
			AuthURL:  "https://www.amazon.com/ap/oa",
			TokenURL: "https://api.amazon.com/auth/o2/token",
		},
		ClientID:     rcloneClientID,
		ClientSecret: fs.Reveal(rcloneEncryptedClientSecret),
		RedirectURL:  oauthutil.RedirectURL,
	}
)

// Register with Fs
func init() {
	fs.Register(&fs.RegInfo{
		Name:        "amazon cloud drive",
		Description: "Amazon Cloud Drive",
		NewFs:       NewFs,
		Config: func(name string) {
			err := oauthutil.Config("amazon cloud drive", name, acdConfig)
			if err != nil {
				log.Fatalf("Failed to configure token: %v", err)
			}
		},
		Options: []fs.Option{{
			Name: fs.ConfigClientID,
			Help: "Amazon Application Client Id - leave blank normally.",
		}, {
			Name: fs.ConfigClientSecret,
			Help: "Amazon Application Client Secret - leave blank normally.",
		}},
	})
	pflag.VarP(&tempLinkThreshold, "acd-templink-threshold", "", "Files >= this size will be downloaded via their tempLink.")
}

// Fs represents a remote acd server
type Fs struct {
	name         string             // name of this remote
	c            *acd.Client        // the connection to the acd server
	noAuthClient *http.Client       // unauthenticated http client
	root         string             // the path we are working on
	dirCache     *dircache.DirCache // Map of directory path to directory id
	pacer        *pacer.Pacer       // pacer for API calls
}

// Object describes a acd object
//
// Will definitely have info but maybe not meta
type Object struct {
	fs     *Fs       // what this object is part of
	remote string    // The remote path
	info   *acd.Node // Info from the acd object if known
}

// ------------------------------------------------------------

// Name of the remote (as passed into NewFs)
func (f *Fs) Name() string {
	return f.name
}

// Root of the remote (as passed into NewFs)
func (f *Fs) Root() string {
	return f.root
}

// String converts this Fs to a string
func (f *Fs) String() string {
	return fmt.Sprintf("Amazon cloud drive root '%s'", f.root)
}

// Pattern to match a acd path
var matcher = regexp.MustCompile(`^([^/]*)(.*)$`)

// parsePath parses an acd 'url'
func parsePath(path string) (root string) {
	root = strings.Trim(path, "/")
	return
}

// retryErrorCodes is a slice of error codes that we will retry
var retryErrorCodes = []int{
	400, // Bad request (seen in "Next token is expired")
	401, // Unauthorized (seen in "Token has expired")
	408, // Request Timeout
	429, // Rate exceeded.
	500, // Get occasional 500 Internal Server Error
	503, // Service Unavailable
	504, // Gateway Time-out
}

// shouldRetry returns a boolean as to whether this resp and err
// deserve to be retried.  It returns the err as a convenience
func shouldRetry(resp *http.Response, err error) (bool, error) {
	if err == io.EOF {
		return true, err
	}
	return fs.ShouldRetry(err) || fs.ShouldRetryHTTP(resp, retryErrorCodes), err
}

// NewFs constructs an Fs from the path, container:path
func NewFs(name, root string) (fs.Fs, error) {
	root = parsePath(root)
	oAuthClient, err := oauthutil.NewClient(name, acdConfig)
	if err != nil {
		log.Fatalf("Failed to configure amazon cloud drive: %v", err)
	}

	c := acd.NewClient(oAuthClient)
	c.UserAgent = fs.UserAgent
	f := &Fs{
		name:         name,
		root:         root,
		c:            c,
		pacer:        pacer.New().SetMinSleep(minSleep).SetPacer(pacer.AmazonCloudDrivePacer),
		noAuthClient: fs.Config.Client(),
	}

	// Update endpoints
	var resp *http.Response
	err = f.pacer.Call(func() (bool, error) {
		_, resp, err = f.c.Account.GetEndpoints()
		return shouldRetry(resp, err)
	})
	if err != nil {
		return nil, fmt.Errorf("Failed to get endpoints: %v", err)
	}

	// Get rootID
	var rootInfo *acd.Folder
	err = f.pacer.Call(func() (bool, error) {
		rootInfo, resp, err = f.c.Nodes.GetRoot()
		return shouldRetry(resp, err)
	})
	if err != nil || rootInfo.Id == nil {
		return nil, fmt.Errorf("Failed to get root: %v", err)
	}

	f.dirCache = dircache.New(root, *rootInfo.Id, f).SetCaseInsensitive()

	// Find the current root
	err = f.dirCache.FindRoot(false)
	if err != nil {
		// Assume it is a file
		newRoot, remote := dircache.SplitPath(root)
		newF := *f
		newF.dirCache = dircache.New(newRoot, *rootInfo.Id, &newF)
		newF.root = newRoot
		// Make new Fs which is the parent
		err = newF.dirCache.FindRoot(false)
		if err != nil {
			// No root so return old f
			return f, nil
		}
		obj := newF.newFsObjectWithInfo(remote, nil)
		if obj == nil {
			// File doesn't exist so return old f
			return f, nil
		}
		// return a Fs Limited to this object
		return fs.NewLimited(&newF, obj), nil
	}
	return f, nil
}

// Return an FsObject from a path
//
// May return nil if an error occurred
func (f *Fs) newFsObjectWithInfo(remote string, info *acd.Node) fs.Object {
	o := &Object{
		fs:     f,
		remote: remote,
	}
	if info != nil {
		// Set info but not meta
		o.info = info
	} else {
		err := o.readMetaData() // reads info and meta, returning an error
		if err != nil {
			// logged already FsDebug("Failed to read info: %s", err)
			return nil
		}
	}
	return o
}

// NewFsObject returns an FsObject from a path
//
// May return nil if an error occurred
func (f *Fs) NewFsObject(remote string) fs.Object {
	return f.newFsObjectWithInfo(remote, nil)
}

// FindLeaf finds a directory of name leaf in the folder with ID pathID
func (f *Fs) FindLeaf(pathID, leaf string) (pathIDOut string, found bool, err error) {
	//fs.Debug(f, "FindLeaf(%q, %q)", pathID, leaf)
	folder := acd.FolderFromId(pathID, f.c.Nodes)
	var resp *http.Response
	var subFolder *acd.Folder
	err = f.pacer.Call(func() (bool, error) {
		subFolder, resp, err = folder.GetFolder(leaf)
		return shouldRetry(resp, err)
	})
	if err != nil {
		if err == acd.ErrorNodeNotFound {
			//fs.Debug(f, "...Not found")
			return "", false, nil
		}
		//fs.Debug(f, "...Error %v", err)
		return "", false, err
	}
	if subFolder.Status != nil && *subFolder.Status != statusAvailable {
		fs.Debug(f, "Ignoring folder %q in state %q", leaf, *subFolder.Status)
		time.Sleep(1 * time.Second) // FIXME wait for problem to go away!
		return "", false, nil
	}
	//fs.Debug(f, "...Found(%q, %v)", *subFolder.Id, leaf)
	return *subFolder.Id, true, nil
}

// CreateDir makes a directory with pathID as parent and name leaf
func (f *Fs) CreateDir(pathID, leaf string) (newID string, err error) {
	//fmt.Printf("CreateDir(%q, %q)\n", pathID, leaf)
	folder := acd.FolderFromId(pathID, f.c.Nodes)
	var resp *http.Response
	var info *acd.Folder
	err = f.pacer.Call(func() (bool, error) {
		info, resp, err = folder.CreateFolder(leaf)
		return shouldRetry(resp, err)
	})
	if err != nil {
		//fmt.Printf("...Error %v\n", err)
		return "", err
	}
	//fmt.Printf("...Id %q\n", *info.Id)
	return *info.Id, nil
}

// list the objects into the function supplied
//
// If directories is set it only sends directories
// User function to process a File item from listAll
//
// Should return true to finish processing
type listAllFn func(*acd.Node) bool

// Lists the directory required calling the user function on each item found
//
// If the user fn ever returns true then it early exits with found = true
func (f *Fs) listAll(dirID string, title string, directoriesOnly bool, filesOnly bool, fn listAllFn) (found bool, err error) {
	query := "parents:" + dirID
	if directoriesOnly {
		query += " AND kind:" + folderKind
	} else if filesOnly {
		query += " AND kind:" + fileKind
	} else {
		// FIXME none of these work
		//query += " AND kind:(" + fileKind + " OR " + folderKind + ")"
		//query += " AND (kind:" + fileKind + " OR kind:" + folderKind + ")"
	}
	opts := acd.NodeListOptions{
		Filters: query,
	}
	var nodes []*acd.Node
	//var resp *http.Response
OUTER:
	for {
		var resp *http.Response
		err = f.pacer.Call(func() (bool, error) {
			nodes, resp, err = f.c.Nodes.GetNodes(&opts)
			return shouldRetry(resp, err)
		})
		if err != nil {
			fs.Stats.Error()
			fs.ErrorLog(f, "Couldn't list files: %v", err)
			break
		}
		if nodes == nil {
			break
		}
		for _, node := range nodes {
			if node.Name != nil && node.Id != nil && node.Kind != nil && node.Status != nil {
				// Ignore nodes if not AVAILABLE
				if *node.Status != statusAvailable {
					continue
				}
				if fn(node) {
					found = true
					break OUTER
				}
			}
		}
	}
	return
}

// Path should be directory path either "" or "path/"
//
// List the directory using a recursive list from the root
//
// This fetches the minimum amount of stuff but does more API calls
// which makes it slow
func (f *Fs) listDirRecursive(dirID string, path string, out fs.ObjectsChan) error {
	var subError error
	// Make the API request
	var wg sync.WaitGroup
	_, err := f.listAll(dirID, "", false, false, func(node *acd.Node) bool {
		// Recurse on directories
		switch *node.Kind {
		case folderKind:
			wg.Add(1)
			folder := path + *node.Name + "/"
			fs.Debug(f, "Reading %s", folder)
			go func() {
				defer wg.Done()
				err := f.listDirRecursive(*node.Id, folder, out)
				if err != nil {
					subError = err
					fs.ErrorLog(f, "Error reading %s:%s", folder, err)
				}
			}()
			return false
		case fileKind:
			if fs := f.newFsObjectWithInfo(path+*node.Name, node); fs != nil {
				out <- fs
			}
		default:
			// ignore ASSET etc
		}
		return false
	})
	wg.Wait()
	fs.Debug(f, "Finished reading %s", path)
	if err != nil {
		return err
	}
	if subError != nil {
		return subError
	}
	return nil
}

// Path should be directory path either "" or "path/"
//
// List the directory using a recursive list from the root
//
// This fetches the minimum amount of stuff but does more API calls
// which makes it slow
func (f *Fs) listDirNonRecursive(dirID string, path string, out fs.ObjectsChan) error {
	// Start some directory listing go routines
	var wg sync.WaitGroup         // sync closing of go routines
	var traversing sync.WaitGroup // running directory traversals
	type dirListJob struct {
		dirID string
		path  string
	}
	in := make(chan dirListJob, fs.Config.Checkers)
	errs := make(chan error, fs.Config.Checkers)
	for i := 0; i < fs.Config.Checkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range in {
				var jobs []dirListJob
				fs.Debug(f, "Reading %q", job.path)
				// Make the API request
				_, err := f.listAll(job.dirID, "", false, false, func(node *acd.Node) bool {
					// Recurse on directories
					switch *node.Kind {
					case folderKind:
						jobs = append(jobs, dirListJob{dirID: *node.Id, path: job.path + *node.Name + "/"})
					case fileKind:
						if fs := f.newFsObjectWithInfo(job.path+*node.Name, node); fs != nil {
							out <- fs
						}
					default:
						// ignore ASSET etc
					}
					return false
				})
				fs.Debug(f, "Finished reading %q", job.path)
				if err != nil {
					fs.ErrorLog(f, "Error reading %s: %s", path, err)
					errs <- err
				}
				// FIXME stop traversal on error?
				traversing.Add(len(jobs))
				go func() {
					// Now we have traversed this directory, send these jobs off for traversal in
					// the background
					for _, job := range jobs {
						in <- job
					}
				}()
				traversing.Done()
			}
		}()
	}

	// Collect the errors
	wg.Add(1)
	var errResult error
	go func() {
		defer wg.Done()
		for err := range errs {
			errResult = err
		}
	}()

	// Start the process
	traversing.Add(1)
	in <- dirListJob{dirID: dirID, path: path}
	traversing.Wait()
	close(in)
	close(errs)
	wg.Wait()

	return errResult
}

// List walks the path returning a channel of FsObjects
func (f *Fs) List() fs.ObjectsChan {
	out := make(fs.ObjectsChan, fs.Config.Checkers)
	go func() {
		defer close(out)
		err := f.dirCache.FindRoot(false)
		if err != nil {
			fs.Stats.Error()
			fs.ErrorLog(f, "Couldn't find root: %s", err)
		} else {
			err = f.listDirNonRecursive(f.dirCache.RootID(), "", out)
			if err != nil {
				fs.Stats.Error()
				fs.ErrorLog(f, "List failed: %s", err)
			}
		}
	}()
	return out
}

// ListDir lists the directories
func (f *Fs) ListDir() fs.DirChan {
	out := make(fs.DirChan, fs.Config.Checkers)
	go func() {
		defer close(out)
		err := f.dirCache.FindRoot(false)
		if err != nil {
			fs.Stats.Error()
			fs.ErrorLog(f, "Couldn't find root: %s", err)
		} else {
			_, err := f.listAll(f.dirCache.RootID(), "", true, false, func(item *acd.Node) bool {
				dir := &fs.Dir{
					Name:  *item.Name,
					Bytes: -1,
					Count: -1,
				}
				dir.When, _ = time.Parse(timeFormat, *item.ModifiedDate)
				out <- dir
				return false
			})
			if err != nil {
				fs.Stats.Error()
				fs.ErrorLog(f, "ListDir failed: %s", err)
			}
		}
	}()
	return out
}

// Put the object into the container
//
// Copy the reader in to the new object which is returned
//
// The new object may have been created if an error is returned
func (f *Fs) Put(in io.Reader, src fs.ObjectInfo) (fs.Object, error) {
	remote := src.Remote()
	size := src.Size()
	// Temporary Object under construction
	o := &Object{
		fs:     f,
		remote: remote,
	}
	leaf, directoryID, err := f.dirCache.FindPath(remote, true)
	if err != nil {
		return nil, err
	}
	if size > warnFileSize {
		fs.Debug(f, "Warning: file %q may fail because it is too big. Use --max-size=%dGB to skip large files.", remote, warnFileSize>>30)
	}
	folder := acd.FolderFromId(directoryID, o.fs.c.Nodes)
	var info *acd.File
	var resp *http.Response
	err = f.pacer.CallNoRetry(func() (bool, error) {
		if src.Size() != 0 {
			info, resp, err = folder.Put(in, leaf)
		} else {
			info, resp, err = folder.PutSized(in, size, leaf)
		}
		return shouldRetry(resp, err)
	})
	if err != nil {
		return nil, err
	}
	o.info = info.Node
	return o, nil
}

// Mkdir creates the container if it doesn't exist
func (f *Fs) Mkdir() error {
	return f.dirCache.FindRoot(true)
}

// purgeCheck remotes the root directory, if check is set then it
// refuses to do so if it has anything in
func (f *Fs) purgeCheck(check bool) error {
	if f.root == "" {
		return fmt.Errorf("Can't purge root directory")
	}
	dc := f.dirCache
	err := dc.FindRoot(false)
	if err != nil {
		return err
	}
	rootID := dc.RootID()

	if check {
		// check directory is empty
		empty := true
		_, err = f.listAll(rootID, "", false, false, func(node *acd.Node) bool {
			switch *node.Kind {
			case folderKind:
				empty = false
				return true
			case fileKind:
				empty = false
				return true
			default:
				fs.Debug("Found ASSET %s", *node.Id)
			}
			return false
		})
		if err != nil {
			return err
		}
		if !empty {
			return fmt.Errorf("Directory not empty")
		}
	}

	node := acd.NodeFromId(rootID, f.c.Nodes)
	var resp *http.Response
	err = f.pacer.Call(func() (bool, error) {
		resp, err = node.Trash()
		return shouldRetry(resp, err)
	})
	if err != nil {
		return err
	}

	f.dirCache.ResetRoot()
	if err != nil {
		return err
	}
	return nil
}

// Rmdir deletes the root folder
//
// Returns an error if it isn't empty
func (f *Fs) Rmdir() error {
	return f.purgeCheck(true)
}

// Precision return the precision of this Fs
func (f *Fs) Precision() time.Duration {
	return fs.ModTimeNotSupported
}

// Hashes returns the supported hash sets.
func (f *Fs) Hashes() fs.HashSet {
	return fs.HashSet(fs.HashMD5)
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
//func (f *Fs) Copy(src fs.Object, remote string) (fs.Object, error) {
// srcObj, ok := src.(*Object)
// if !ok {
// 	fs.Debug(src, "Can't copy - not same remote type")
// 	return nil, fs.ErrorCantCopy
// }
// srcFs := srcObj.fs
// _, err := f.c.ObjectCopy(srcFs.container, srcFs.root+srcObj.remote, f.container, f.root+remote, nil)
// if err != nil {
// 	return nil, err
// }
// return f.NewFsObject(remote), nil
//}

// Purge deletes all the files and the container
//
// Optional interface: Only implement this if you have a way of
// deleting all the files quicker than just running Remove() on the
// result of List()
func (f *Fs) Purge() error {
	return f.purgeCheck(false)
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

// Hash returns the Md5sum of an object returning a lowercase hex string
func (o *Object) Hash(t fs.HashType) (string, error) {
	if t != fs.HashMD5 {
		return "", fs.ErrHashUnsupported
	}
	if o.info.ContentProperties.Md5 != nil {
		return *o.info.ContentProperties.Md5, nil
	}
	return "", nil
}

// Size returns the size of an object in bytes
func (o *Object) Size() int64 {
	return int64(*o.info.ContentProperties.Size)
}

// readMetaData gets the metadata if it hasn't already been fetched
//
// it also sets the info
func (o *Object) readMetaData() (err error) {
	if o.info != nil {
		return nil
	}
	leaf, directoryID, err := o.fs.dirCache.FindPath(o.remote, false)
	if err != nil {
		return err
	}
	folder := acd.FolderFromId(directoryID, o.fs.c.Nodes)
	var resp *http.Response
	var info *acd.File
	err = o.fs.pacer.Call(func() (bool, error) {
		info, resp, err = folder.GetFile(leaf)
		return shouldRetry(resp, err)
	})
	if err != nil {
		fs.Debug(o, "Failed to read info: %s", err)
		return err
	}
	o.info = info.Node
	return nil
}

// ModTime returns the modification time of the object
//
//
// It attempts to read the objects mtime and if that isn't present the
// LastModified returned in the http headers
func (o *Object) ModTime() time.Time {
	err := o.readMetaData()
	if err != nil {
		fs.Log(o, "Failed to read metadata: %s", err)
		return time.Now()
	}
	modTime, err := time.Parse(timeFormat, *o.info.ModifiedDate)
	if err != nil {
		fs.Log(o, "Failed to read mtime from object: %s", err)
		return time.Now()
	}
	return modTime
}

// SetModTime sets the modification time of the local fs object
func (o *Object) SetModTime(modTime time.Time) {
	// FIXME not implemented
	return
}

// Storable returns a boolean showing whether this object storable
func (o *Object) Storable() bool {
	return true
}

// Open an object for read
func (o *Object) Open() (in io.ReadCloser, err error) {
	bigObject := o.Size() >= int64(tempLinkThreshold)
	if bigObject {
		fs.Debug(o, "Dowloading large object via tempLink")
	}
	file := acd.File{Node: o.info}
	var resp *http.Response
	err = o.fs.pacer.Call(func() (bool, error) {
		if !bigObject {
			in, resp, err = file.Open()
		} else {
			in, resp, err = file.OpenTempURL(o.fs.noAuthClient)
		}
		return shouldRetry(resp, err)
	})
	return in, err
}

// Update the object with the contents of the io.Reader, modTime and size
//
// The new object may have been created if an error is returned
func (o *Object) Update(in io.Reader, src fs.ObjectInfo) error {
	size := src.Size()
	file := acd.File{Node: o.info}
	var info *acd.File
	var resp *http.Response
	var err error
	err = o.fs.pacer.CallNoRetry(func() (bool, error) {
		if size != 0 {
			info, resp, err = file.OverwriteSized(in, size)
		} else {
			info, resp, err = file.Overwrite(in)
		}
		return shouldRetry(resp, err)
	})
	if err != nil {
		return err
	}
	o.info = info.Node
	return nil
}

// Remove an object
func (o *Object) Remove() error {
	var resp *http.Response
	var err error
	err = o.fs.pacer.Call(func() (bool, error) {
		resp, err = o.info.Trash()
		return shouldRetry(resp, err)
	})
	return err
}

// Check the interfaces are satisfied
var (
	_ fs.Fs     = (*Fs)(nil)
	_ fs.Purger = (*Fs)(nil)
	//	_ fs.Copier   = (*Fs)(nil)
	//	_ fs.Mover    = (*Fs)(nil)
	//	_ fs.DirMover = (*Fs)(nil)
	_ fs.Object = (*Object)(nil)
)
