package aliyunimpl

import (
	"github.com/allspace/csmgr/common"
)

type sliceFile struct {
	fscommon.SliceFile
}

func NewSliceFile(io fscommon.FileIO) *sliceFile {
	sf := &sliceFile{}
	sf.SetIO(io)
	return sf
}
