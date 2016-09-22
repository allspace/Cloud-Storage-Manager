package s3impl
import (
	//"fmt"
	"log"
	"time"
	"sync"
	"container/list"
	
	//"github.com/aws/aws-sdk-go/service/s3"
	"brightlib.com/common"
)

const(
	FILE_BLOCK_SIZE			 = 5*1024*1024
	FILE_SLICE_SIZE			 = 5*1024*1024*1024
	
	S3_MIN_BLOCK_SIZE        = 5*1024*1024
	S3_MAX_BLOCK_SIZE        = 5*1024*1024*1024
)

const (
	TASK_STOP 		= 1
	TASK_READ_BLOCK	= 2
	TASK_COMBINE_T1	= 3
	TASK_COMBINE_T2 = 4
	TASK_PAUSE_CLEAN	= 5
	TASK_RESUME_CLEAN	= 6
)

type cacheBuffer struct {
	buffer 		[]byte
	bufferLen 	int
	offset 		int64
	fullOffset  int64
	maxOffset   int64
}

type dataBlock cacheBuffer
type taskCmd struct {
	cmd		int
	blknum  int64
	data	[]byte
	
	waitChan (chan int)
}

//this is a file implimentation for S3
//it will be accessed by multiple threads, and will be referenced by multiple user space file handles
type remoteCache struct {
	appendBuffer *cacheBuffer
	totalAppendSize  int64
	appendBlocks		 []int64
	appendBlockStartOffset int64
	appendBlockCount     int
	
	modifyBuffer *cacheBuffer
	buffAllocMtx sync.Mutex
	io *S3FileIO
	fs *S3FileSystemImpl
	
	file 			*sliceFile
	fileName 		string
	openFlags		uint32
	fileLen 		int64
	filePersisentLen int64
	mtxOpen sync.Mutex
	mtxWrite sync.Mutex
	distWriteList map[int64]int
	
	//for block maintain task
	mtTaskStarted 	bool
	mtChan			(chan taskCmd)
	
	modified		bool 			//the file had been modified
	modBlockList 	*list.List		//modified block list
	cacheCleanMtx   sync.Mutex
} 

func newRemoteCache(name string, io *S3FileIO, fs *S3FileSystemImpl) *remoteCache {	
	return &remoteCache{
		io : io,
		fs : fs,
		file : nil,
		fileName: name,
		fileLen: 0,
		
		appendBuffer    : newCacheBuffer(),
		appendBlocks        : make([]int64, 1024),
		appendBlockStartOffset : 0,
		distWriteList       : make(map[int64]int),
		
		mtTaskStarted   : false,
		mtChan          : make(chan taskCmd),
	}
}

func newCacheBuffer() *cacheBuffer {
	bsize := FILE_BLOCK_SIZE + 1024 * 1024		//6 MB
	return &cacheBuffer{
		buffer 		: make([]byte, bsize),
		bufferLen 	: bsize,
		offset		: 0,
		fullOffset  : 0,
		maxOffset	: 0,
	}
}

func (me *remoteCache) Open(fileName string, flags uint32)int{
	var ok int = 0
	
	if me.file == nil {
		me.mtxOpen.Lock()
		if me.file == nil {
			me.file = NewSliceFile(me.io)
			ok = me.file.Open(fileName, flags)
			if ok == 0 {
				me.appendBuffer.offset     = me.file.GetLength()
				me.appendBuffer.fullOffset = me.appendBuffer.offset 
				me.appendBuffer.maxOffset  = me.appendBuffer.offset
				me.appendBlockStartOffset  = me.file.GetLength()
				me.appendBlockCount = 0
				me.fileLen = me.file.GetLength()				
			}else{
				me.file = nil
			}
		}
		me.mtxOpen.Unlock()
	}
	return ok
}

func (me *remoteCache) Read(dest []byte, offset int64)(int) {
	remainLen := len(dest)
	curOffset := offset
	curDest   := dest

	//offset falls into base file, or even later
	if curOffset < me.file.GetLength() {
		n := me.file.Read(curDest, curOffset)
		if n < 0 {
			return n
		}
		remainLen -= n
		curOffset += int64(n)
		curDest = curDest[n:]
	}
	
	//offset falls into cache block scope, or even later
	for remainLen > 0 && me.appendBlockCount > 0 && curOffset >= me.appendBlockStartOffset && curOffset < me.appendBuffer.offset {
		//figure out fall into which block
		blkIdx := int(((curOffset - me.appendBlockStartOffset) + FILE_BLOCK_SIZE) / FILE_BLOCK_SIZE)
		blkOffset := me.appendBlocks[blkIdx]
		start := curOffset - blkOffset
		fileName := me.file.getCacheBlockFileName(blkOffset)
		n := me.io.getBuffer(fileName, curDest, start)
		if n < 0 {
			return n
		}
		remainLen -= n
		curOffset += int64(n)
		curDest = curDest[n:]
	}
	

	//offset falls into append buffer scope
	if remainLen > 0 && curOffset >= me.appendBuffer.offset {
		start := int(curOffset - me.appendBuffer.offset)
		n := 0
		if me.appendBuffer.maxOffset - curOffset >= int64(len(curDest)) {
			n = len(curDest)
		}else{
			n = int(me.appendBuffer.maxOffset - curOffset)
		}
		copy(curDest, me.appendBuffer.buffer[start: start + n])
		remainLen -= n
		curOffset += int64(n)
	}
	
	return (len(dest) - remainLen)
}

//write function
func (me *remoteCache) Write(data []byte, offset int64) int {
	//"write" must be serialized
	me.mtxWrite.Lock()
	defer me.mtxWrite.Unlock()
	
	log.Printf("Write is called for file %s, offset=%d, data length=%d\n", me.fileName, offset, len(data))
	
	if me.mtTaskStarted == false {
		//go me.blockMaintainTask()
		me.mtTaskStarted = true
	}
	
	count := len(data)
	if count > 0 {
		me.modified = true
	}
	
	//append case
	log.Printf("me.fileLen=%d\n", me.fileLen)
	//IMPORTANT: the offset may not continous
	if offset >= me.fileLen {
		log.Printf("Append file, file length=%d\n", me.fileLen)
		return me.appendFile(data, offset)
	}
	
	log.Println("Run into unsupported cases for file ", me.fileName)
	
	//random write case
	if offset < me.fileLen {
		return me.randomWrite(data, offset)
	}
	
	//truncate to enlarge case, not support for now
	if offset > me.fileLen {
		return -1
	}
	
	return -1
}

func (me *remoteCache) Flush()int {
	log.Printf("Flush is called for file %s\n", me.fileName)
	
	//readonly
	if me.modified != true {
		return 0
	}
	
	dataLen := me.appendBuffer.maxOffset - me.appendBuffer.offset
	
	log.Printf("Flush pendding write: me.appendBlockCount=%d, dataLen=%d\n", 
				me.appendBlockCount, dataLen)
	
	return me.file.Append(me.appendBlocks[0:me.appendBlockCount], 
							me.appendBuffer.buffer[0:dataLen])
	
	//run parts combination
	//tc := taskCmd{cmd: TASK_COMBINE_T2, waitChan: make(chan int)}
	//me.mtChan <- tc
	//rs := <- tc.waitChan
	//return rs
}

//no IO should be involved in this release function
func (me *remoteCache) Release()int {
	me.appendBuffer = nil
	me.appendBlocks = nil
	return 0
}

func (me *remoteCache) Utimens(Mtime *time.Time)(int) {
	return 0
}

func (me *remoteCache) Truncate(size uint64)int {
	ok := me.file.Truncate(size)
	if ok < 0 {
		return ok
	}
	me.fileLen = me.file.GetLength()
	if me.fileLen == 0 {
		me.appendBuffer.offset = me.file.GetLength()
		me.appendBlockStartOffset = me.file.GetLength()
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
		task = <- me.mtChan	//wait for command
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
				break;
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


func (me *remoteCache) appendFile(data []byte, offset int64) int {

	log.Printf("AppendFile is called for file %s, offset=%d, data length=%d", me.fileName, offset, len(data))
	
	//allocate append buffer if not yet
	if me.appendBuffer == nil {
		me.buffAllocMtx.Lock()
		if me.appendBuffer == nil {
			me.appendBuffer = newCacheBuffer()
		}
		me.buffAllocMtx.Unlock()
	}
	
	remainN := len(data)
	curData := data
	curOffset := offset
	
	for remainN > 0 {
		//save data to buffer first
		start := int(curOffset - me.appendBuffer.offset)
		freeN := me.appendBuffer.bufferLen - start
		if freeN <= 0 {
			log.Printf("There must be something wrong: offset = %d\n", curOffset)
			return fscommon.EIO
		}
		copyN := 0
		if freeN > remainN {
			copyN = remainN
		} else {
			copyN = freeN
		}
		
		buff := me.appendBuffer.buffer[start:]
		copy(buff, curData[0:copyN])
		
		//let's see if we can move fullOffset forward
		n := curOffset + int64(copyN) - me.appendBuffer.fullOffset
		if curOffset <= me.appendBuffer.fullOffset && n > 0 {
			me.appendBuffer.fullOffset += n
			
			//fullOffset get moved, check list
			for off,m := range me.distWriteList {
				if off <= me.appendBuffer.fullOffset {
					if off + int64(m) > me.appendBuffer.fullOffset {
						me.appendBuffer.fullOffset += (off + int64(m) - me.appendBuffer.fullOffset)
					} 
					delete(me.distWriteList, off)
				}
			}
		
		}else{
			me.distWriteList[curOffset] = copyN			//discontinous write, add to list
		}
		
		if curOffset + int64(copyN) > me.appendBuffer.maxOffset {
			me.appendBuffer.maxOffset = curOffset + int64(copyN)
		}
				
		curOffset += int64(copyN)
		remainN -= copyN
		curData = curData[copyN:]
		me.fileLen = me.appendBuffer.maxOffset	//file length
		
		//buffer data length reach FILE_BLOCK_SIZE, trigger T1 block uploading
		dLen := me.appendBuffer.fullOffset - me.appendBuffer.offset
		if dLen >= FILE_BLOCK_SIZE {
			
			//upload the buffer
			name := me.file.getCacheBlockFileName(me.appendBuffer.offset)
			ok := me.io.putFile(name, me.appendBuffer.buffer[0:dLen])
			if ok < 0 {		//we cannot move forward if buffer cannot be uploaded
				log.Println("Failed to upload buffer to file ", name)
				return ok
			}
			
			me.appendBuffer.offset += FILE_BLOCK_SIZE
			me.appendBlockCount += 1
			//TODO: clean unused buffer area
			copy(me.appendBuffer.buffer, me.appendBuffer.buffer[dLen:])
		}
	}
	
	//the number of tier 1 block reach uplimit, start the background combination thread
	if me.appendBlockCount >= 1024 {
		//go me.combineT1Blocks(me.t1BlockNumStart, 1024)
		//send end number to "file block maintain task"
		//select {
		//	case me.mtChan <- taskCmd{cmd: TASK_COMBINE_T1, blknum: me.t1BlockNumEnd}:	
		//	default:
		//}
		ok := me.file.Append(me.appendBlocks[:1024], nil)
		if ok < 0 {
			log.Printf("Failed to commit cache blocks to file %s.\n", me.fileName)
		}else{
			//free entries
			for i := 0; i < 1024; i++ {
				if me.appendBlocks[i]>=0 {
					me.io.Unlink(me.file.getCacheBlockFileName(me.appendBlocks[i]))
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
	//allocate modify buffer if not yet
	if me.modifyBuffer == nil {
		me.buffAllocMtx.Lock()
		if me.modifyBuffer == nil {
			me.modifyBuffer = newCacheBuffer()
		}
		me.buffAllocMtx.Unlock()
	}
	
	//count := len(data)
	//blknum := (offset + FILE_BLOCK_SIZE) / FILE_BLOCK_SIZE
	
	return 0
}


func (me *remoteCache) GetInfo()(*fscommon.DirItem) {
	return &fscommon.DirItem{
		Name : me.fileName,
		Size : uint64(me.fileLen),
		Mtime: time.Now(),
		Type : fscommon.S_IFREG,
	}
}
