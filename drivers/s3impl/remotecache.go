package s3impl
import (
	"fmt"
	"log"
	"time"
	"sync"
	"container/list"
	
	"github.com/aws/aws-sdk-go/service/s3"
	//"brightlib.com/common"
)

const(
	DEFAULT_CACHE_BUFFER_LEN = 5*1024*1024
	FILE_BLOCK_SIZE			 = 5*1024*1024
	T2_BLOCK_SIZE			 = 5*1024*1024*1024
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
	dataLen 	int
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
	appendBlockNumStart	 int64
	appendBlockNumEnd	 int64
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
		appendBlockNumStart : 0,
		appendBlockNumEnd   : 0,
		appendBlocks        : make([]int64, 1024),
		appendBlockStartOffset : 0,
		
		mtTaskStarted   : false,
		mtChan          : make(chan taskCmd),
	}
}

func newCacheBuffer() *cacheBuffer {
	bsize := FILE_BLOCK_SIZE
	return &cacheBuffer{
		buffer 		: make([]byte, bsize),
		bufferLen 	: bsize,
		offset		: 0,
		dataLen		: 0,
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
				me.appendBuffer.offset = me.file.GetLength()
				me.appendBlockStartOffset = me.file.GetLength()
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
		fileName := me.file.getBlockFileName(blkOffset)
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
		if me.appendBuffer.dataLen - start >= len(curDest) {
			n = len(curDest)
		}else{
			n = me.appendBuffer.dataLen - start
		}
		copy(curDest, me.appendBuffer.buffer[start: start + n])
		remainLen -= n
		curOffset += int64(n)
	}
	
	return (len(dest) - remainLen)
}

//write function
func (me *remoteCache) Write(data []byte, offset int64) int {
	log.Printf("Write is called for file %s, offset=%d, data length=%d\n", me.fileName, offset, len(data))
	
	if me.mtTaskStarted == false {
		go me.blockMaintainTask()
		me.mtTaskStarted = true
	}
	
	count := len(data)
	if count > 0 {
		me.modified = true
	}
	
	//append case
	log.Printf("me.fileLen=%d\n", me.fileLen)
	if offset == me.fileLen {
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
	
	log.Printf("Flush pendding write: me.appendBlockCount=%d, me.appendBuffer.dataLen=%d\n", 
				me.appendBlockCount, me.appendBuffer.dataLen)
				
	return me.file.Append(me.appendBlocks[0:me.appendBlockCount], 
							me.appendBuffer.buffer[0:me.appendBuffer.dataLen])
	
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
	
	for remainN > 0 {
		//save data to buffer first
		freeN := me.appendBuffer.bufferLen - me.appendBuffer.dataLen
		copyN := 0
		if freeN > remainN {
			copyN = remainN
		} else {
			copyN = freeN
		}
		
		buff := me.appendBuffer.buffer[me.appendBuffer.dataLen:]
		copy(buff, curData[0:copyN])
		
		me.appendBuffer.dataLen += copyN
		remainN -= copyN
		curData = curData[copyN:]
		me.fileLen += int64(copyN)
		
		//buffer is full, trigger T1 block uploading
		if me.appendBuffer.dataLen == me.appendBuffer.bufferLen {
			
			//upload the buffer
			name := me.file.getBlockFileName(me.appendBuffer.offset)
			ok := me.io.putFile(name, me.appendBuffer.buffer)
			if ok != 0 {		//we cannot move forward if buffer cannot be uploaded
				log.Println("Failed to upload buffer to file ", name)
				return ok
			}
			
			me.appendBuffer.offset += FILE_BLOCK_SIZE
			me.appendBlockCount += 1
			me.appendBuffer.dataLen = 0
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

//upload last part of the file
//it may be the whole file itself
/* func (me *remoteCache) uploadLastPart(name string)(int) {
	if me.appendBuffer.dataLen == 0 {
		return 0
	}
	
	if len(name) == 0 {
		name = fmt.Sprintf("$cache$/%s.T1_%d", me.fileName, me.t1BlockNumEnd)
	}
	
	ok := me.io.putFile(name, me.appendBuffer.buffer[0:me.appendBuffer.dataLen])
	if ok < 0 {		//we cannot move forward if buffer cannot be uploaded
		log.Println("Failed to upload last part for file ", name)
		return ok
	}
	return 0
} */

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

//"offset" is base on start point of cache
/* func (me *remoteCache) readAppendCache(dest []byte, offset int64)(int) {
	var n int64 = 0
	var count = len(dest)
	var rc int
	var readLen int = 0
	
	//really need look into append buffer now
	//get data from maintain task
	blknum := (offset+FILE_BLOCK_SIZE) / FILE_BLOCK_SIZE
	
	//It's in T1 block. so all data will be in T1 blocks
	if blknum >= me.t1BlockNumStart {
		for count>0 {	//loop until all data is read, or reach EOF
			name := fmt.Sprintf("$cache$/%s.T1_%d", me.fileName, blknum)
			blkoff := offset % FILE_BLOCK_SIZE
			rc = me.io.getBuffer(name, dest[n:], blkoff)
			if rc != int(n) {
				return rc
			}
			count -= rc		//reduce got length
			offset += int64(rc)
			readLen += rc
			blknum++
		}
		return readLen
	//at least part of data is in T2 block
	} else if blknum < me.t1BlockNumStart {
		blknum := (offset+FILE_BLOCK_SIZE*1024) / (FILE_BLOCK_SIZE*1024)
		if blknum < me.t2BlockNumStart {
			log.Println("There is something wrong with block scope.")
			return -1
		}
		for count > 0 {
			name := fmt.Sprintf("$cache$/%s.T2_%d", me.fileName, blknum)
			blkoff := offset % (FILE_BLOCK_SIZE*1024)
			rc = me.io.getBuffer(name, dest[n:], blkoff)
			if rc != int(n) {
				return rc
			}
			count -= rc		//reduce got length
			offset += int64(rc)
			readLen += rc
			blknum++
		}
	}
	return 0
} */

func (me *remoteCache) combineFileForAppend(oriFile string, fsize int64)(int) {	
	log.Printf("combineFileForAppend is called for file %s, fsize=%d", oriFile, fsize)
	var ok int
	var sliceSize int64		//track current slice size
	
	//original file size is less than 5MB, so we cannot use multipart copy here
	if fsize < FILE_BLOCK_SIZE {
		
		//special case: only one block in append buffer, and original file does not exist, or its size is zero
		if (fsize <= 0) && (me.appendBlockNumEnd == me.appendBlockNumStart) {
			ok = me.io.putFile(oriFile, me.appendBuffer.buffer[0:me.appendBuffer.dataLen])
			if ok < 0 {
				return ok
			}else{
				return 0
			}
		}
		
		tmpBuff := make([]byte, 2*FILE_BLOCK_SIZE)
		//download original file
		var n int = 0;
		if fsize > 0 {
			n = me.io.getBuffer(me.fileName, tmpBuff, 0)
		}else{
			n = 0		//for case original file does not exist, or its size is zero
		}
		
		//download 1st append block (it may be in append buffer if there is only one append block)
		var m int = 0;
		if me.appendBlockNumEnd > me.appendBlockNumStart {
			name := fmt.Sprintf("$cache$/%s.A1_%d", me.fileName, me.appendBlockNumStart)
			m = me.io.getBuffer(name, tmpBuff[n:], 0)
		}else{
			m = me.appendBuffer.dataLen
			copy(tmpBuff[n:], me.appendBuffer.buffer[0:m])			
		}
		
		//upload the combined version
		ok = me.io.putFile(oriFile, tmpBuff[0:n+m])
		if ok < 0 {
			return ok
		}
		
		//no additional blocks, we are ready to return
		if me.appendBlockNumEnd == 0 {
			return 0
		}
		
		sliceSize = int64(n + m)
	}else{
		sliceSize = fsize
	}
	
	//at this stage, we are sure that last file slice is bigger than FILE_BLOCK_SIZE, so it can participate
	//in a multipart copy process
	
	//a multipart copy process
	tgt := fmt.Sprintf("$cache$/%s_tmp")
	uploadId,ok := me.io.startUpload(tgt)
	if ok < 0 {
		return ok
	}
	plist := make([]*s3.CompletedPart, 2)
	
	//original file is the 1st part of this multipart copy process
	part, ok := me.io.copyPart(tgt, oriFile, "", uploadId, 1)
	plist = append(plist, part)
	
	var pnum int64 = 2
	startNum := me.appendBlockNumStart - 1
	for i:=startNum; i< me.appendBlockNumEnd; i++ {
		
		if sliceSize + FILE_BLOCK_SIZE >= FILE_SLICE_SIZE {
			//reach slice size, need commit the current slice
		}
	
		name := fmt.Sprintf("$cache$/%s.A1_%d", me.fileName, i)
		part, ok := me.io.copyPart(tgt, name, "", uploadId, pnum)
		if ok != 0 {
			break
		}
		plist = append(plist, part)
		pnum++
	}
	
	//there is data left in append buffer, upload it as the last part
	if me.appendBuffer.dataLen > 0 {
		part, ok = me.io.uploadPart(tgt, me.appendBuffer.buffer[0:me.appendBuffer.dataLen], uploadId, pnum)
		if ok != 0 {
			return ok
		}
		plist = append(plist, part)
	}
	
	//commit the copy operation
	ok = me.io.completeUpload(tgt, uploadId, plist)
	if ok != 0 {
		return ok
	}
	
	ok = me.io.Rename(tgt, oriFile)
	
	//remove append blocks
	for i:=me.appendBlockNumStart; i< me.appendBlockNumEnd; i++ {
		name := fmt.Sprintf("$cache$/%s.A1_%d", me.fileName, i)
		me.io.Unlink(name)
	}
	
	return ok
}


