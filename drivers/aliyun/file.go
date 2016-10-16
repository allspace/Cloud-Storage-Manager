package aliyunimpl

import (
	"log"
	"sync"

	"github.com/allspace/csmgr/common"
)

type AliyunFile struct {
	fscommon.FileImplBase

	io *AliyunIO

	mtxOpen  sync.Mutex
	mtxWrite sync.Mutex
}

func NewAliyunFile(io *AliyunIO) *AliyunFile {
	return &AliyunFile{
		io: io,
	}
}

func (me *AliyunFile) Open(fileName string, flags uint32) int {
	me.FileName = fileName
	me.OpenFlags = flags

	var ok int = 0

	if me.File == nil {
		me.mtxOpen.Lock()
		if me.File == nil {
			me.File = NewSliceFile(me.io)
			ok = me.File.Open(fileName, flags)
			if ok == 0 {
				me.FileLen = me.File.GetLength()
			} else {
				me.File = nil
			}
		}
		me.mtxOpen.Unlock()
	}
	return ok
}

//write function
func (me *AliyunFile) Write(data []byte, offset int64) int {
	//"write" must be serialized
	me.mtxWrite.Lock()
	defer me.mtxWrite.Unlock()

	log.Printf("Write is called for file %s, offset=%d, data length=%d\n", me.FileName, offset, len(data))

	count := len(data)
	if count > 0 {
		me.Modified = true
	}

	//append case
	log.Printf("me.FileLen=%d\n", me.FileLen)
	//IMPORTANT: the offset may not continous
	if offset >= me.AppendBuffer.BaseOffset {
		log.Printf("Append file, file length=%d\n", me.FileLen)
		//return me.appendFile(data, offset)
	}

	log.Println("Run into unsupported cases for file ", me.FileName)

	//random write case
	//if offset < me.appendBuffer.BaseOffset {
	//	return me.randomWrite(data, offset)
	//}

	//truncate to enlarge case, not support for now
	if offset > me.FileLen {
		return -1
	}

	return -1
}

func (me *AliyunFile) Flush() int {
	log.Printf("Flush is called for file %s\n", me.FileName)

	//readonly
	if me.Modified != true {
		return 0
	}

	dataLen := me.AppendBuffer.MaxOffset - me.AppendBuffer.BaseOffset

	log.Printf("Flush pendding write: dataLen=%d\n", dataLen)

	return me.File.Append(nil, me.AppendBuffer.Buffer[0:dataLen])

	//run parts combination
	//tc := taskCmd{cmd: TASK_COMBINE_T2, waitChan: make(chan int)}
	//me.mtChan <- tc
	//rs := <- tc.waitChan
	//return rs
}

func (me *AliyunFile) appendFile(data []byte, offset int64) int {
	return me.FileImplBase.Append(data, offset)
}

func (me *AliyunFile) onAppendBufferFull(data []byte, offset int64) int {
	return 0
}
