package fscommon

import (
    "time"
	"sync"
)

type FileImplBase struct {
	FileName 	string
	FileLen  	int64
	OpenFlags   uint32
	
	File         ISliceFile
	AppendBuffer *CacheBuffer
	buffAllocMtx sync.Mutex
	
	Modified     bool
}

//no IO should be involved in this release function
func (me *FileImplBase) Release()int {
	return 0
}

func (me *FileImplBase) Utimens(Mtime *time.Time)(int) {
	return 0
}

func (me *FileImplBase) Truncate(size uint64)int {
	ok := me.File.Truncate(size)
	if ok < 0 {
		return ok
	}
	me.FileLen = me.File.GetLength()
	if me.FileLen == 0 {
		me.AppendBuffer.ResetOffset(me.File.GetLength())
	}	
	return 0
}

func (me *FileImplBase) Append(data []byte, offset int64)int {
    if me.AppendBuffer == nil {
		me.buffAllocMtx.Lock()
		if me.AppendBuffer == nil {
			me.AppendBuffer = NewCacheBuffer(0, nil, 0)
		}
		me.buffAllocMtx.Unlock()
	}
	
	return me.AppendBuffer.Write(data, offset)
}

func (me *FileImplBase) GetInfo()(*DirItem) {
	return &DirItem{
		Name : me.FileName,
		Size : uint64(me.FileLen),
		Mtime: time.Now(),
		Type : S_IFREG,
	}
}
