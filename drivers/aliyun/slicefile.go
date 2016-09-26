package aliyunimpl
import (
    "brightlib.com/common"
)

type sliceFile struct {
    fscommon.SliceFile
}

func NewSliceFile(io fscommon.FileIO) *sliceFile {
    sf := &sliceFile{}
	sf.SetIO(io)
	return sf
}
