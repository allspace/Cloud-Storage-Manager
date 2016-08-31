package fscommon

import(
	"time"
)


const (
	S_IFUNKOWN = 0
	S_IFREG = 1
	S_IFDIR = 2
)

const (
	OK		= 0
	EPERM	= 1
	ENOENT	= 2
)



type DirItem struct {
	Name 	string
	Size 	uint64
	Mode 	uint32
	Mtime	time.Time
    Uid     uint32
    Gid     uint32
	Type	int
}

type FsInfo struct {
    Blocks  uint64
    Bfree   uint64
    Bavail  uint64
    Files   uint64
    Ffree   uint64
    Bsize   uint32
}

type FileImpl interface {
	Read(dest []byte, off int64)(int) 
}


type FileSystemImpl interface {
	ReadDir(path string) ([]DirItem , int)
	GetAttr(path string)(*DirItem, int)
	Open(path string, flags uint32)(FileImpl, int)
	StatFs(name string)(*FsInfo, int)
	Unlink(path string)(int)
	Mkdir(path string, mode uint32)(int)
}

