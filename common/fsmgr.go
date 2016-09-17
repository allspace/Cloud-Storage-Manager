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
	EPERM	= -1
	ENOENT	= -2
	EIO     = -5
	EBUSY   = -16
	EEXIST  = -17
	ENOTDIR = -20
	EISDIR  = -21
	EINVAL  = -22
	ENOSYS  = -38
)

const (
	O_CREAT = 0x00000040
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
	Write(data []byte, off int64)(int) 
	Flush()(int)
	Open(path string, flags uint32)(int)
	Utimens(Mtime *time.Time)(int) 
	Truncate(size uint64) int
	
	//no I/O should be involved in these functions
	//they are run in big lock context
	Release()(int)
}


type FileSystemImpl interface {
	NewFileImpl(path string)(FileImpl, int)
		
	ReadDir(path string) ([]DirItem , int)
	GetAttr(path string)(*DirItem, int)
	Open(path string, flags uint32)(*FileObject, int)
	StatFs(name string)(*FsInfo, int)
	Unlink(path string)(int)
	Mkdir(path string, mode uint32)(int)
}

