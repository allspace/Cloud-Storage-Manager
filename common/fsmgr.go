package fscommon

import(
	"time"
	"log"
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

type ClientImpl interface {
	Set(key string, value string) 
	Connect(region string, keyId string, keyData string)(int) 
	Mount(bucketName string)(FileSystemImpl, int)
}

type FileImpl interface {
	GetInfo()(*DirItem) 
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


//trim the last slash if there is
//get the last component of a path
func GetLastPathComp(path string) string {
	var end int
	if path[len(path)-1] == '/' {
		end = len(path) - 2
	}else{
		end = len(path) - 1
	}
	if end == -1 {		//means path == "/"
		return ""
	}
	idx := 0	//if slash is not found, then we will use the string start from 0
	for i := end; i >= 0; i-- {
		if path[i] == '/' {
			idx = i + 1
			break
		}
	}
	
	log.Println("***", path, ": idx=", idx, ", end=", end)
	return path[idx : end + 1]
}
