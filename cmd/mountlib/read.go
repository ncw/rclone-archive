package mountlib

import (
	"fmt"
	"io"
	"sync"

	"github.com/hashicorp/golang-lru"
	"github.com/ncw/rclone/fs"
	"github.com/pkg/errors"
)

// ReadFileHandle is an open for read file handle on a File
type ReadFileHandle struct {
	mu         sync.Mutex
	closed     bool // set if handle has been closed
	r          *fs.Account
	o          fs.Object
	readCalled bool // set if read has been called
	offset     int64
	noSeek     bool
	file       *File
	hash       *fs.MultiHasher
	opened     bool
}

func newReadFileHandle(f *File, o fs.Object) (*ReadFileHandle, error) {
	var hash *fs.MultiHasher
	var err error
	if !f.d.fsys.noChecksum {
		hash, err = fs.NewMultiHasherTypes(o.Fs().Hashes())
		if err != nil {
			fs.Errorf(o.Fs(), "newReadFileHandle hash error: %v", err)
		}
	}

	fh := &ReadFileHandle{
		o:      o,
		noSeek: f.d.fsys.noSeek,
		file:   f,
		hash:   hash,
	}
	fs.Stats.Transferring(fh.o.Remote())
	return fh, nil
}

// openPending opens the file if there is a pending open
// call with the lock held
func (fh *ReadFileHandle) openPending() (err error) {
	if fh.opened {
		return nil
	}
	r, err := fh.o.Open()
	if err != nil {
		return err
	}
	fh.r = fs.NewAccount(r, fh.o).WithBuffer() // account the transfer
	fh.opened = true
	return nil
}

// String converts it to printable
func (fh *ReadFileHandle) String() string {
	if fh == nil {
		return "<nil *ReadFileHandle>"
	}
	if fh.file == nil {
		return "<nil *ReadFileHandle.file>"
	}
	return fh.file.String() + " (r)"
}

// Node returns the Node assocuated with this - satisfies Noder interface
func (fh *ReadFileHandle) Node() Node {
	return fh.file
}

// FIXME global variables
var lru_cache, _ = lru.New(100)
var chunksize int64 = 16777216

// seek to a new offset
//
// if reopen is true, then we won't attempt to use an io.Seeker interface
//
// Must be called with fh.mu held
func (fh *ReadFileHandle) seek(offset int64, reopen bool) (err error) {
	if fh.noSeek {
		return ESPIPE
	}
	fh.r.StopBuffering() // stop the background reading first
	fh.hash = nil
	oldReader := fh.r.GetReader()
	r := oldReader
	// Can we seek it directly?
	if do, ok := oldReader.(io.Seeker); !reopen && ok {
		fs.Debugf(fh.o, "ReadFileHandle.seek from %d to %d (io.Seeker)", fh.offset, offset)
		_, err = do.Seek(offset, 0)
		if err != nil {
			fs.Debugf(fh.o, "ReadFileHandle.Read io.Seeker failed: %v", err)
			return err
		}
	} else {
		fs.Debugf(fh.o, "ReadFileHandle.seek from %d to %d", fh.offset, offset)
		// close old one
		err = oldReader.Close()
		if err != nil {
			fs.Debugf(fh.o, "ReadFileHandle.Read seek close old failed: %v", err)
		}
		// re-open with a seek
		r, err = fh.o.Open(&fs.SeekOption{Offset: offset})
		if err != nil {
			fs.Debugf(fh.o, "ReadFileHandle.Read seek failed: %v", err)
			return err
		}
	}
	fh.r.UpdateReader(r)
	fh.offset = offset
	return nil
}

// getChunk reads a chunk from the file handle into the LRU cache
func (fh *ReadFileHandle) getChunk(chunk int64, key string) (ret []byte, err error) {
	var tries = 0
	var newOffset int64
	var reopen = false
	ret = make([]byte, chunksize)
	for {
		if fh.offset != (chunk*chunksize) || reopen {
			err = fh.seek(chunk*chunksize, reopen)
		}

		if err == nil {
			var n, err = io.ReadFull(fh.r, ret)
			newOffset = fh.offset + int64(n)
			if err != nil && !((err == io.ErrUnexpectedEOF || err == io.EOF) && newOffset == fh.o.Size()) {
				fs.Errorf(fh.o, "ReadFileHandle.Read error: %v chunk[%d]", err, chunk)
				reopen = true
			} else {
				err = nil
				fh.offset = newOffset
				lru_cache.Add(key, ret)
				fs.Debugf(fh.o, "ReadFileHandle.Read get chunk[%d]", chunk)
				return ret, err
			}
		} else {
			reopen = true
		}

		tries++

		if tries > fs.Config.LowLevelRetries {
			break
		}
	}
	fs.Errorf(fh.o, "ReadFileHandle.Read error: %v", err)
	return nil, err
}

// Read from the file handle
func (fh *ReadFileHandle) Read(reqSize, reqOffset int64) (respData []byte, err error) {
	fh.mu.Lock()
	defer fh.mu.Unlock()
	err = fh.openPending() // FIXME pending open could be more efficient in the presense of seek (and retried)
	if err != nil {
		return nil, err
	}
	// fs.Debugf(fh.o, "ReadFileHandle.Read size %d offset %d", reqSize, reqOffset)
	if fh.closed {
		fs.Errorf(fh.o, "ReadFileHandle.Read error: %v", EBADF)
		return nil, EBADF
	}
	var n int
	var newOffset int64
	retries := 0
	buf := make([]byte, reqSize)
	doReopen := false
	for {
		// Does the read span at most 2 chunks?
		if int64(reqSize) <= chunksize && fh.o.Size() > chunksize {
			var chunkstart, chunkend int64
			// Starting chunk for read
			chunkstart = reqOffset / chunksize
			// Ending chunk for read
			chunkend = (reqOffset + int64(reqSize)) / chunksize

			var chunks []byte
			var chunke []byte

			// Cache key used
			var key string = fmt.Sprintf("%v-%d", fh.o, chunkstart)
			var key_next string = fmt.Sprintf("%v-%d", fh.o, chunkstart+1)

			// Look for chunk in LRU Cache
			var buffer, cached = lru_cache.Get(key)
			if cached {
				// Use chunk from cache
				fs.Debugf(fh.o, "ReadFileHandle.Read cached[%d]", chunkstart)
				chunks = buffer.([]byte)
			} else {
				// Fill cache with chunk
				chunks, err = fh.getChunk(chunkstart, key)
				go fh.getChunk(chunkstart+1, key_next)
			}

			if reqSize > 0 {
				fh.readCalled = true
			}

			if err != nil {
				return nil, err
			}

			if chunkend != chunkstart {
				// Start and end chunk differ, read end chunk and join

				key = fmt.Sprintf("%v-%d", fh.o, chunkend)

				var buffer, cached = lru_cache.Get(key)
				if cached {
					fs.Debugf(fh.o, "ReadFileHandle.Read cached[%d]", chunkend)
					chunke = buffer.([]byte)
				} else {
					chunke, err = fh.getChunk(chunkend, key)
				}

				if err == nil {
					// Offsets into chunks
					var start = (reqOffset - (chunkstart * chunksize))
					var end = ((reqOffset + int64(reqSize)) - (chunkend * chunksize))

					respData = append(chunks[start:], chunke[:end]...)
					fs.Debugf(fh.o, "ReadFileHandle.Read OK[cached]")
				}
				return respData, err

			} else {
				// All data in one chunk, use it
				respData = chunks[(reqOffset - (chunkstart * chunksize)):((reqOffset + int64(reqSize)) - (chunkstart * chunksize))]
				fs.Debugf(fh.o, "ReadFileHandle.Read OK[cached]")
				return respData, err
			}
		}

		doSeek := reqOffset != fh.offset

		if doSeek {
			// Are we attempting to seek beyond the end of the
			// file - if so just return EOF leaving the underlying
			// file in an unchanged state.
			if reqOffset >= fh.o.Size() {
				fs.Debugf(fh.o, "ReadFileHandle.Read attempt to read beyond end of file: %d > %d", reqOffset, fh.o.Size())
				return nil, nil
			}
			// Otherwise do the seek
			err = fh.seek(reqOffset, doReopen)
		} else {
			err = nil
		}
		if err == nil {
			if reqSize > 0 {
				fh.readCalled = true
			}
			// One exception to the above is if we fail to fully populate a
			// page cache page; a read into page cache is always page aligned.
			// Make sure we never serve a partial read, to avoid that.
			n, err = io.ReadFull(fh.r, buf)
			newOffset = fh.offset + int64(n)
			// if err == nil && rand.Intn(10) == 0 {
			// 	err = errors.New("random error")
			// }
			if err == nil {
				break
			} else if (err == io.ErrUnexpectedEOF || err == io.EOF) && newOffset == fh.o.Size() {
				// Have read to end of file - reset error
				err = nil
				break
			}
		}
		if retries >= fs.Config.LowLevelRetries {
			break
		}
		retries++
		fs.Errorf(fh.o, "ReadFileHandle.Read error: low level retry %d/%d: %v", retries, fs.Config.LowLevelRetries, err)
		doSeek = true
		doReopen = true
	}
	if err != nil {
		fs.Errorf(fh.o, "ReadFileHandle.Read error: %v", err)
	} else {
		respData = buf[:n]
		fh.offset = newOffset
		// fs.Debugf(fh.o, "ReadFileHandle.Read OK")

		if fh.hash != nil {
			_, err = fh.hash.Write(respData)
			if err != nil {
				fs.Errorf(fh.o, "ReadFileHandle.Read HashError: %v", err)
				return nil, err
			}
		}
	}
	return respData, err
}

func (fh *ReadFileHandle) checkHash() error {
	if fh.hash == nil || !fh.readCalled || fh.offset < fh.o.Size() {
		return nil
	}

	for hashType, dstSum := range fh.hash.Sums() {
		srcSum, err := fh.o.Hash(hashType)
		if err != nil {
			return err
		}
		if !fs.HashEquals(dstSum, srcSum) {
			return errors.Errorf("corrupted on transfer: %v hash differ %q vs %q", hashType, dstSum, srcSum)
		}
	}

	return nil
}

// close the file handle returning EBADF if it has been
// closed already.
//
// Must be called with fh.mu held
func (fh *ReadFileHandle) close() error {
	if fh.closed {
		return EBADF
	}
	fh.closed = true
	fs.Stats.DoneTransferring(fh.o.Remote(), true)

	if err := fh.checkHash(); err != nil {
		return err
	}

	return fh.r.Close()
}

// Flush is called each time the file or directory is closed.
// Because there can be multiple file descriptors referring to a
// single opened file, Flush can be called multiple times.
func (fh *ReadFileHandle) Flush() error {
	fh.mu.Lock()
	defer fh.mu.Unlock()
	if !fh.opened {
		return nil
	}
	// fs.Debugf(fh.o, "ReadFileHandle.Flush")

	if err := fh.checkHash(); err != nil {
		fs.Errorf(fh.o, "ReadFileHandle.Flush error: %v", err)
		return err
	}

	// fs.Debugf(fh.o, "ReadFileHandle.Flush OK")
	return nil
}

// Release is called when we are finished with the file handle
//
// It isn't called directly from userspace so the error is ignored by
// the kernel
func (fh *ReadFileHandle) Release() error {
	fh.mu.Lock()
	defer fh.mu.Unlock()
	if !fh.opened {
		return nil
	}
	if fh.closed {
		fs.Debugf(fh.o, "ReadFileHandle.Release nothing to do")
		return nil
	}
	fs.Debugf(fh.o, "ReadFileHandle.Release closing")
	err := fh.close()
	if err != nil {
		fs.Errorf(fh.o, "ReadFileHandle.Release error: %v", err)
	} else {
		// fs.Debugf(fh.o, "ReadFileHandle.Release OK")
	}
	return err
}
