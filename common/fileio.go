package fscommon

type FileIO interface {
    PutBuffer(name string, data []byte)(int)
	GetBuffer(name string, dest []byte, offset int64)(int)
	GetRemoteFileSize(path string)(int64)
	ListFile(path string) ([]DirItem , int) 
	ZeroFile(name string)(int)
	Unlink(path string)(int) 
}