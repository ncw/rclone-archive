package webdav

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDirCache(t *testing.T) {
	f := Fs{
		dirCache: make(map[string]struct{}, 10),
	}
	assert.Equal(t, map[string]struct{}{}, f.dirCache)
	f.dirCacheAddDir("dir/")
	assert.Equal(t, map[string]struct{}{
		"dir/": struct{}{},
	}, f.dirCache)
	f.dirCacheAddDir("dir/subdir/")
	f.dirCacheAddDir("dir2/")
	assert.Equal(t, map[string]struct{}{
		"dir/":        struct{}{},
		"dir2/":       struct{}{},
		"dir/subdir/": struct{}{},
	}, f.dirCache)
	assert.Equal(t, true, f.dirCacheExists("dir/"))
	assert.Equal(t, true, f.dirCacheExists("dir/subdir/"))
	assert.Equal(t, false, f.dirCacheExists("dirX/"))
	f.dirCacheRemoveDirAll("dir/")
	assert.Equal(t, map[string]struct{}{
		"dir2/": struct{}{},
	}, f.dirCache)
	f.dirCacheRemoveDir("dir2/")
	assert.Equal(t, map[string]struct{}{}, f.dirCache)
}
