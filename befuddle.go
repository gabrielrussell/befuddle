// Hellofs implements a simple "hello world" file system.
package main

import (
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	_ "bazil.org/fuse/fs/fstestutil"
	"bytes"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"labix.org/v2/mgo/bson"
	"log"
	"os"
)

var Usage = func() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s <mountpoint> <bsonfile>\n", os.Args[0])
	flag.PrintDefaults()
}

// Step through all docs and return rawD
func mongoDumpToRawD(dump []byte) (rawD bson.RawD, err error) {
	buf := bytes.NewReader(dump)

	var i = 0
	var nextLoc int64 = 0
	rawD = bson.RawD{}
	for nextLoc < int64(len(dump)) {
		var docLen int32 = 0

		err := binary.Read(buf, binary.LittleEndian, &docLen)
		if err != nil {
			return nil, errors.New("Read error for docLen")
		}
		// Process doc
		var raw bson.Raw
		var rawDoc bson.RawDocElem
		raw.Kind = 0x03
		raw.Data = dump[nextLoc : nextLoc+int64(docLen)]
		rawDoc.Name = fmt.Sprintf("Doc %v", i)
		rawDoc.Value = raw
		rawD = append(rawD, rawDoc)

		// Should use size of docLen and not 4!
		nextLoc = int64(docLen - 4)
		nextLoc, err = buf.Seek(nextLoc, 1)
		if err != nil {
			return nil, errors.New("Fell off end of file")
		}
		i++
	}
	return rawD, nil
}

// Dir implements both Node and Handle for the root directory.
type Dir struct {
	children []DFNode
	//	bson     bson.Raw
	rd     bson.RawD
	attr   fuse.Attr
	dirent fuse.Dirent
	name   string
}

// File implements both Node and Handle for the hello file.
type File struct {
	data   []byte
	attr   fuse.Attr
	dirent fuse.Dirent
	name   string
}

type DFNode interface {
	Dirent() fuse.Dirent
	Attr() fuse.Attr
}

var rootDir Dir
var inode uint64 = 1

func main() {
	flag.Usage = Usage
	flag.Parse()

	if flag.NArg() != 2 {
		Usage()
		os.Exit(2)
	}
	mountpoint := flag.Arg(0)
	bsonFile := flag.Arg(1)

	bsonData, err := ioutil.ReadFile(bsonFile)

	if err != nil {
		log.Panic(err)
	}

	//bsonRaw := bson.Raw{3, bsonData}

	var bsonRawD bson.RawD

	bsonRawD, err = mongoDumpToRawD(bsonData)
	if err != nil {
		log.Panic(err)
	}

	if err != nil {
		log.Panic(err)
	}

	rootDir.rd = bsonRawD
	rootDir.attr = fuse.Attr{Inode: inode, Mode: os.ModeDir | 0555}

	c, err := fuse.Mount(mountpoint)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	err = fs.Serve(c, FS{})
	if err != nil {
		log.Fatal(err)
	}

	// check if the mount process has an error to report
	<-c.Ready
	if err := c.MountError; err != nil {
		log.Fatal(err)
	}
}

// FS implements the hello world file system.
type FS struct{}

func (FS) Root() (fs.Node, fuse.Error) {
	return rootDir, nil
}

func (d Dir) Attr() fuse.Attr {
	return d.attr
}

func (d Dir) Lookup(name string, intr fs.Intr) (fs.Node, fuse.Error) {
	d.fleshOut()
	for i := range d.rd {
		if name == d.rd[i].Name {
			return d.children[i], nil
		}
	}
	return nil, fuse.ENOENT
}

func (d Dir) ReadDir(intr fs.Intr) ([]fuse.Dirent, fuse.Error) {
	d.fleshOut()
	de := make([]fuse.Dirent, len(d.children), len(d.children))
	for i, v := range d.children {
		de[i] = v.Dirent()
	}
	return de, nil
}

func (d *Dir) fleshOut() {
	if len(d.children) != len(d.rd) {
		d.children = rawDToDFNodes(d.rd)
	}
}

func (d Dir) Dirent() fuse.Dirent {
	return d.dirent
}

func rawDToDFNodes(rd bson.RawD) []DFNode {
	nodes := make([]DFNode, len(rd), len(rd))
	for i, v := range rd {
		if v.Value.Kind == 3 {
			var d Dir
			v.Value.Unmarshal(&d.rd)
			d.attr = fuse.Attr{Inode: inode, Mode: os.ModeDir | 0555}
			d.name = v.Name
			d.dirent = fuse.Dirent{Inode: inode, Name: v.Name, Type: fuse.DT_Dir}
			nodes[i] = d
			inode++
		} else {
			var f File
			f.data = v.Value.Data
			f.name = v.Name
			f.attr = fuse.Attr{Inode: inode, Mode: 0444, Size: uint64(len(f.data))}
			f.dirent = fuse.Dirent{Inode: inode, Name: v.Name, Type: fuse.DT_File}
			nodes[i] = f
			inode++
		}
	}
	return nodes
}

func (f File) Attr() fuse.Attr {
	return f.attr
}

func (f File) ReadAll(intr fs.Intr) ([]byte, fuse.Error) {
	return f.data, nil
}

func (f File) Dirent() fuse.Dirent {
	return f.dirent
}
