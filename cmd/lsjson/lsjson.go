package lsjson

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"time"

	"github.com/ncw/rclone/cmd"
	"github.com/ncw/rclone/fs"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

var (
	recurse   bool
	showHash  bool
	noModTime bool
)

func init() {
	cmd.Root.AddCommand(commandDefintion)
	commandDefintion.Flags().BoolVarP(&recurse, "recursive", "R", false, "Recurse into the listing.")
	commandDefintion.Flags().BoolVarP(&showHash, "hash", "", false, "Include hashes in the output (may take longer).")
	commandDefintion.Flags().BoolVarP(&noModTime, "no-modtime", "", false, "Don't read the modification time (can speed things up).")
}

// lsJSON in the struct which gets marshalled for each line
type lsJSON struct {
	Path    string
	Name    string
	Size    int64
	ModTime Timestamp //`json:",omitempty"`
	IsDir   bool
	Hashes  map[string]string `json:",omitempty"`
}

// Timestamp a time in RFC3339 format with Nanosecond precision secongs
type Timestamp time.Time

// MarshalJSON turns a Timestamp into JSON
func (t Timestamp) MarshalJSON() (out []byte, err error) {
	tt := time.Time(t)
	if tt.IsZero() {
		return []byte(`""`), nil
	}
	return []byte(`"` + tt.Format(time.RFC3339Nano) + `"`), nil
}

var commandDefintion = &cobra.Command{
	Use:   "lsjson remote:path",
	Short: `List directories and objects in the path in JSON format.`,
	Long: `List directories and objects in the path in JSON format.

The output is an array of Items, where each Item looks like this

   {
      "Hashes" : {
         "SHA-1" : "f572d396fae9206628714fb2ce00f72e94f2258f",
         "MD5" : "b1946ac92492d2347c6235b4d2611184",
         "DropboxHash" : "ecb65bb98f9d905b70458986c39fcbad7715e5f2fcc3b1f07767d7c83e2438cc"
      },
      "IsDir" : false,
      "ModTime" : "2017-05-31T16:15:57.034468261+01:00",
      "Name" : "file.txt",
      "Path" : "full/path/goes/here/file.txt",
      "Size" : 6
   }

If --hash is not specified the the Hashes property won't be emitted.

If --no-modtime is specified then ModTime will be blank.

The time is in RFC3339 format with nanosecond precision.

The whole output can be processed as a JSON blob, or alternatively it
can be processed line by line as each item is written one to a line.

Here is an example of the output on this directory tree

    /tmp/ls
    ├── directory
    │   └── file3.txt
    ├── file1.txt
    └── file2.txt

    $ rclone lsjson /tmp/ls
    [
    {"Path":"directory","Name":"directory","Size":-1,"ModTime":"2017-07-10T11:04:28.271497613+01:00","IsDir":true},
    {"Path":"file1.txt","Name":"file1.txt","Size":9,"ModTime":"2017-07-10T11:04:01.43147201+01:00","IsDir":false},
    {"Path":"file2.txt","Name":"file2.txt","Size":9,"ModTime":"2017-07-10T11:04:03.991474451+01:00","IsDir":false}
    ]

    $ rclone lsjson -R /tmp/ls
    [
    {"Path":"directory","Name":"directory","Size":-1,"ModTime":"2017-07-10T11:04:28.271497613+01:00","IsDir":true},
    {"Path":"file1.txt","Name":"file1.txt","Size":9,"ModTime":"2017-07-10T11:04:01.43147201+01:00","IsDir":false},
    {"Path":"file2.txt","Name":"file2.txt","Size":9,"ModTime":"2017-07-10T11:04:03.991474451+01:00","IsDir":false},
    {"Path":"directory/file3.txt","Name":"file3.txt","Size":9,"ModTime":"2017-07-10T11:04:28.271497613+01:00","IsDir":false}
    ]

So if you want to parse it line by line, ignore lines with only "[" and
"]" and strip any trailng commas off lines to get a valid JSON object.

`,
	Run: func(command *cobra.Command, args []string) {
		cmd.CheckArgs(1, 1, command, args)
		fsrc := cmd.NewFsSrc(args)
		cmd.Run(false, false, command, func() error {
			fmt.Println("[")
			first := true
			err := fs.Walk(fsrc, "", false, fs.ConfigMaxDepth(recurse), func(dirPath string, entries fs.DirEntries, err error) error {
				if err != nil {
					fs.Stats.Error()
					fs.Errorf(dirPath, "error listing: %v", err)
					return nil
				}
				for _, entry := range entries {
					item := lsJSON{
						Path: entry.Remote(),
						Name: path.Base(entry.Remote()),
						Size: entry.Size(),
					}
					if !noModTime {
						item.ModTime = Timestamp(entry.ModTime())
					}
					switch x := entry.(type) {
					case fs.Directory:
						item.IsDir = true
					case fs.Object:
						item.IsDir = false
						if showHash {
							item.Hashes = make(map[string]string)
							for _, hashType := range x.Fs().Hashes().Array() {
								hash, err := x.Hash(hashType)
								if err != nil {
									fs.Errorf(x, "Failed to read hash: %v", err)
								} else if hash != "" {
									item.Hashes[hashType.String()] = hash
								}
							}
						}
					default:
						fs.Errorf(nil, "Unknown type %T in listing", entry)
					}
					out, err := json.Marshal(item)
					if err != nil {
						return errors.Wrap(err, "failed to marshal list object")
					}
					if first {
						first = false
					} else {
						fmt.Print(",\n")
					}
					_, err = os.Stdout.Write(out)
					if err != nil {
						return errors.Wrap(err, "failed to write to output")
					}

				}
				return nil
			})
			if err != nil {
				return errors.Wrap(err, "error listing JSON")
			}
			if !first {
				fmt.Println()
			}
			fmt.Println("]")
			return nil
		})
	},
}
