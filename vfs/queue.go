package vfs

import (
	"sync"

	"github.com/ncw/rclone/fs"
)

// queue implments an upload queue for queuedFile
//
// queued files aren't transferred immediately as we wait for any more
// changes to come in.
//
// this can be serialized to disk..
type queue struct {
	mu         sync.Mutex
	vfs        *VFS
	files      []queuedFile
	inProgress int
}

// queuedFile represents a file that needs to be uploaded
type queuedFile struct {
	localPath string    // locally cached object
	dst       fs.Object // dst Object if it exists
	remote    string    // dst Remote name
	src       fs.Object
}

func (q *queue) addFile() {
}
