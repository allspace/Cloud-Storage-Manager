package main

import (
	"fmt"
	"time"
	//"os"
	//"flag"
	"log"
	
	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
	"brightlib.com/common"
)


type HelloFs struct {
	pathfs.FileSystem
	FileSystemImpl fscommon.FileSystemImpl
}

type HelloFile struct {
	nodefs.File
	fileObject *fscommon.FileObject
}



func (me *HelloFs) getMode(mode int)(uint32) {
	if mode == fscommon.S_IFDIR {
		return fuse.S_IFDIR | 0755
	}else{
		return fuse.S_IFREG | 0644
	}
}

func (me *HelloFs) GetAttr(name string, context *fuse.Context) (*fuse.Attr, fuse.Status) {

	//root of mount point
	//go-fuse is different than c-fuse: there is no leading slash for every full path
	if name == "" {		
		return &fuse.Attr{
			Mode: fuse.S_IFDIR | 0755,
			Size: 0,
			Mtime: uint64(time.Now().Unix()),
		}, fuse.OK
	}
	
	//get attributes from cache or remote
	di,ok := me.FileSystemImpl.GetAttr(name)
	if ok==0 {
		return &fuse.Attr{
			Mode: me.getMode(di.Type),
			Size: di.Size,
			Mtime: uint64(di.Mtime.Unix()),
		}, fuse.OK
	}else{
		return nil, fuse.ENOENT
	}
}

func (me *HelloFs) OpenDir(name string, context *fuse.Context) (c []fuse.DirEntry, code fuse.Status) {
	//
	fmt.Println("***OpenDir: ", name)
	
	dirs,n := me.FileSystemImpl.ReadDir(name)
	if n<0 {
		return nil, fuse.ENOENT
	}
	
	c = make([]fuse.DirEntry, n)
	for i := range dirs {
		//if empty(dirs[i].Name) {
		//	continue
		//}
		c[i].Name = dirs[i].Name
		c[i].Mode = me.getMode(dirs[i].Type)
		fmt.Println(dirs[i].Name)
	}
	
	return c, fuse.OK
}

func (me *HelloFs) Open(name string, flags uint32, context *fuse.Context) (file nodefs.File, code fuse.Status) {
	
	fh, ok := me.FileSystemImpl.Open(name, flags)
	if ok==0 {
		log.Println("Open file ", name, " successfully.")
		return &HelloFile{fileObject: fh}, fuse.OK
	}else{
		log.Println("Failed to open ", name)
		return nil, fuse.ENOENT
	}
}

func (me *HelloFs) Create(name string, flags uint32, mode uint32, context *fuse.Context) (file nodefs.File, code fuse.Status) {
	return me.Open(name, flags, context)
}

func (me *HelloFs) StatFs(name string) *fuse.StatfsOut {
	fi, ok := me.FileSystemImpl.StatFs(name)
	if ok != 0 {
		return nil
	}else{
		return &fuse.StatfsOut{
			Blocks	: fi.Blocks,
			Bfree	: fi.Bfree,
			Bavail	: fi.Bavail,
			Bsize	: fi.Bsize,
		}
	}
}

func (me *HelloFs) Unlink(name string, context *fuse.Context) (code fuse.Status) {
	ok := me.FileSystemImpl.Unlink(name)
	if ok != 0 {
		return fuse.ENOENT
	}else{
		return fuse.OK
	}
}

func (me *HelloFs) Mkdir(name string, mode uint32, context *fuse.Context) fuse.Status {
	ok := me.FileSystemImpl.Mkdir(name, mode)
	if ok != 0 {
		return fuse.ENOENT
	}else{
		return fuse.OK
	}
}

func (me *HelloFs) Chmod(name string, mode uint32, context *fuse.Context) (code fuse.Status) {
	return fuse.OK
}


///////////////////////////////////////////////////////////////////////////////
//For file related functions
///////////////////////////////////////////////////////////////////////////////


func (me *HelloFile) SetInode(*nodefs.Inode) {

}

func (me *HelloFile) GetAttr(out *fuse.Attr) fuse.Status {
	return fuse.EBADF
}

func (me *HelloFile) Utimens(atime *time.Time, mtime *time.Time) fuse.Status {
	return fuse.OK
}

func (me *HelloFile) Flush() fuse.Status {
	rc := me.fileObject.Flush()
	if rc==0 {
		return fuse.OK
	}else{
		return fuse.EIO
	}
}

//there should not be any real IO in this function
func (me *HelloFile) Release() {
	me.fileObject.Release()
	return
}

func (me *HelloFile) Read(dest []byte, off int64) (fuse.ReadResult, fuse.Status) {
	log.Println("Get read request at: ", off, " length ", len(dest))
	log.Println(me.fileObject)
	
	n := me.fileObject.Read(dest, off)
	if n < 0 {
		return nil, fuse.EIO     
	}
	
	log.Println("Read data for ", n)
	return fuse.ReadResultData(dest), fuse.OK
}

func (me *HelloFile) Write(data []byte, off int64) (written uint32, code fuse.Status) {
	n := me.fileObject.Write(data, off)
	if n < 0 {
		return 0, fuse.EIO     
	}
	return uint32(n), fuse.OK
}


///////////////////////////////////////////////////////////////////////////////



