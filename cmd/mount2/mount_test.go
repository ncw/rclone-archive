// +build linux darwin freebsd

package mount2

import (
	"testing"

	"github.com/rclone/rclone/cmd/mountlib/mounttest"
)

func TestMount(t *testing.T) {
	mounttest.RunTests(t, mount)
}
