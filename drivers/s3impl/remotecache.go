package s3impl

import (
	//"fmt"
	"log"
	//"time"
	"container/list"
	"sync"

	"github.com/allspace/csmgr/common"
)

const (
	FILE_BLOCK_SIZE = 5 * 1024 * 1024
	FILE_SLICE_SIZE = 5 * 1024 * 1024 * 1024

	S3_MIN_BLOCK_SIZE = 5 * 1024 * 1024
	S3_MAX_BLOCK_SIZE = 5 * 1024 * 1024 * 1024
)

const (
	TASK_STOP         = 1
	TASK_READ_BLOCK   = 2
	TASK_COMBINE_T1   = 3
	TASK_COMBINE_T2   = 4
	TASK_PAUSE_CLEAN  = 5
	TASK_RESUME_CLEAN = 6
)

type taskCmd struct {
	cmd    int
	blknum int64
	data   []byte

	waitChan (chan int)
}

//this is a file implimentation for S3
//it will be accessed by multiple threads, and will be referenced by multiple user space file handles
type remoteCache struct {
	fscommon.FileImplBase

	appendBlocks           []int64
	appendBlockStartOffset int64
	appendBlockCount       int

	modifyBuffer *fscommon.CacheBuffer

	io *S3FileIO
	fs *S3FileSystemImpl

	mtxOpen  sync.Mutex
	mtxWrite sync.Mutex

	//for block maintain task
	mtTaskStarted bool
	mtChan        (chan taskCmd)

	modBlockList  *list.List //modified block list
	cacheCleanMtx sync.Mutex
}

func newRemoteCache(name string, io *S3FileIO, fs *S3FileSystemImpl) *remoteCache {
	return &remoteCache{
		io: io,
		fs: fs,

		appendBlocks:           make([]int64, 1024),
		appendBlockStartOffset: 0,

		mtTaskStarted: false,
		mtChan:        make(chan taskCmd),
	}
}

func (me *remoteCache) Open(fileName string, flags uint32) int {
	var ok int = 0

	if me.File == nil {
		me.mtxOpen.Lock()
		if me.File == nil {
			me.File = NewSliceFile(me.io)
			ok = me.File.Open(fileName, flags)
			if ok == 0 {
				me.appendBlockStartOffset = me.File.GetLength()
				me.appendBlockCount = 0
				me.FileLen = me.File.GetLength()
			} else {
				me.File = nil
			}
		}
		me.mtxOpen.Unlock()
	}
	return ok
}

func (me *remoteCache) Read(dest []byte, offset int64) int {
	remainLen := len(dest)
	curOffset := offset
	curDest := dest
	var n int = 0

	//offset falls into base file, or even later
	if curOffset < me.File.GetLength() {
		n = me.File.Read(curDest, curOffset)
		if n < 0 {
			return n
		}
		remainLen -= n
		curOffset += int64(n)
		curDest = curDest[n:]
	}

	//offset falls into cache block scope, or even later
	for remainLen > 0 && me.appendBlockCount > 0 && curOffset >= me.appendBlockStartOffset && curOffset < me.AppendBuffer.BaseOffset {
		//figure out fall into which block
		blkIdx := int(((curOffset - me.appendBlockStartOffset) + FILE_BLOCK_SIZE) / FILE_BLOCK_SIZE)
		blkOffset := me.appendBlocks[blkIdx]
		start := curOffset - blkOffset
		fileName := me.File.GetCacheBlockFileName(blkOffset)
		n = me.io.GetBuffer(fileName, curDest, start)
		if n < 0 {
			return n
		}
		remainLen -= n
		curOffset += int64(n)
		curDest = curDest[n:]
	}

	//offset falls into append buffer scope
	n = me.AppendBuffer.Read(curDest, curOffset)
	remainLen -= n
	curOffset += int64(n)

	return (len(dest) - remainLen)
}

//write function
func (me *remoteCache) Write(data []byte, offset int64) int {
	//"write" must be serialized
	me.mtxWrite.Lock()
	defer me.mtxWrite.Unlock()

	log.Printf("Write is called for file %s, offset=%d, data length=%d\n", me.FileName, offset, len(data))

	if me.mtTaskStarted == false {
		//go me.blockMaintainTask()
		me.mtTaskStarted = true
	}

	count := len(data)
	if count > 0 {
		me.Modified = true
	}

	//append case
	log.Printf("me.fileLen=%d\n", me.FileLen)
	//IMPORTANT: the offset may not continous
	if offset >= me.FileLen || (me.AppendBuffer != nil && offset >= me.AppendBuffer.BaseOffset) {
		log.Printf("Append file, file length=%d\n", me.FileLen)
		return me.appendFile(data, offset)
	}

	log.Println("Run into unsupported cases for file ", me.FileName)

	//random write case
	if offset < me.AppendBuffer.BaseOffset {
		return me.randomWrite(data, offset)
	}

	//truncate to enlarge case, not support for now
	if offset > me.FileLen {
		return -1
	}

	return -1
}

func (me *remoteCache) Flush() int {
	log.Printf("Flush is called for file %s\n", me.FileName)

	//readonly
	if me.Modified != true {
		return 0
	}

	dataLen := me.AppendBuffer.GetDataLen()

	log.Printf("Flush pendding write: me.appendBlockCount=%d, dataLen=%d\n",
		me.appendBlockCount, dataLen)

	return me.File.Append(me.appendBlocks[0:me.appendBlockCount],
		me.AppendBuffer.GetData())

	//run parts combination
	//tc := taskCmd{cmd: TASK_COMBINE_T2, waitChan: make(chan int)}
	//me.mtChan <- tc
	//rs := <- tc.waitChan
	//return rs
}

func (me *remoteCache) Truncate(size uint64) int {
	ok := me.FileImplBase.Truncate(size)
	if ok < 0 {
		return ok
	}
	if me.FileLen == 0 {
		me.appendBlockStartOffset = 0
		me.appendBlockCount = 0
	}
	return 0
}

///////////////////////////////////////////////////////////////////////////////
//Internal functions
///////////////////////////////////////////////////////////////////////////////

func (me *remoteCache) blockMaintainTask() {
	var task taskCmd
	var rc int
	for true {
		task = <-me.mtChan //wait for command
		switch task.cmd {
		case TASK_STOP:
			break
		case TASK_READ_BLOCK:
			//rc = me.readBlock(task.data, task.blknum)
			task.waitChan <- rc
			break
		case TASK_COMBINE_T1:
			//verify it again and then start combine process
			// for me.t1BlockNumEnd - me.t1BlockNumStart >= 1024 {
			// me.combineT1Blocks(me.t1BlockNumStart, 1024)
			// }
			break
		case TASK_COMBINE_T2:
			// for me.t1BlockNumEnd - me.t1BlockNumStart > 0 {
			// var count int = 0
			// if me.t1BlockNumEnd - me.t1BlockNumStart > 1024 {
			// count = 1024
			// }else{
			// count = int(me.t1BlockNumEnd - me.t1BlockNumStart)
			// }
			// me.combineT1Blocks(me.t1BlockNumStart, count)
			// }
			// rc := me.combineT2Blocks(me.t2BlockNumStart, int(me.t2BlockNumEnd-me.t2BlockNumStart))
			task.waitChan <- rc
			break
		}
		/*
			if num < 0 {		//got stop signal
				//do the final combine
				me.combineT1Blocks(me.t1BlockNumStart, int(me.t1BlockNumEnd - me.t1BlockNumStart))
				me.combineT2Blocks(me.t2BlockNumStart, int(me.t2BlockNumEnd - me.t2BlockNumStart))
				break
			}
		*/
		//this lock is required when delete cache files
		me.cacheCleanMtx.Lock()
		me.cacheCleanMtx.Unlock()

	}

	me.mtTaskStarted = false
}

func (me *remoteCache) uploadBlock(data []byte, offset int64) int {
	name := me.File.GetCacheBlockFileName(offset)
	ok := me.io.PutBuffer(name, data)
	if ok < 0 {
		return ok
	}
	me.appendBlockCount++
	return 0
}

func (me *remoteCache) appendFile(data []byte, offset int64) int {

	log.Printf("AppendFile is called for file %s, offset=%d, data length=%d", me.FileName, offset, len(data))

	ok := me.FileImplBase.Append(data, offset)
	if ok < 0 {
		return ok
	}
	me.FileLen = me.AppendBuffer.MaxOffset

	//the number of tier 1 block reach uplimit, start the background combination thread
	if me.appendBlockCount >= 1024 {
		//go me.combineT1Blocks(me.t1BlockNumStart, 1024)
		//send end number to "file block maintain task"
		//select {
		//	case me.mtChan <- taskCmd{cmd: TASK_COMBINE_T1, blknum: me.t1BlockNumEnd}:
		//	default:
		//}
		ok := me.File.Append(me.appendBlocks[:1024], nil)
		if ok < 0 {
			log.Printf("Failed to commit cache blocks to file %s.\n", me.FileName)
		} else {
			//free entries
			for i := 0; i < 1024; i++ {
				if me.appendBlocks[i] >= 0 {
					me.io.Unlink(me.File.GetCacheBlockFileName(me.appendBlocks[i]))
				}
				me.appendBlocks[i] = -1
			}

			//move remained items to head
			me.appendBlockCount -= 1024
			if me.appendBlockCount > 0 {
				copy(me.appendBlocks, me.appendBlocks[1024:])
				me.appendBlockStartOffset = me.appendBlocks[me.appendBlockCount-1]
			}
		}
	}

	return len(data)
}

func (me *remoteCache) randomWrite(data []byte, offset int64) int {

	//count := len(data)
	//blknum := (offset + FILE_BLOCK_SIZE) / FILE_BLOCK_SIZE

	return 0
}
