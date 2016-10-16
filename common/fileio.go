package fscommon

import (
	"os"
)

type FileIO interface {
	PutBuffer(name string, data []byte) int
	GetBuffer(name string, dest []byte, offset int64) int
	GetAttr(path string) (os.FileInfo, int)
	ListFile(path string) ([]os.FileInfo, int)
	ZeroFile(name string) int
	Unlink(path string) int
}
