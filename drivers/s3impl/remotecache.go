package s3impl
import (
	"fmt"
	"log"
	//"strconv"
	"sync"
	"container/list"
	
	"github.com/aws/aws-sdk-go/service/s3"
)

const(
	DEFAULT_CACHE_BUFFER_LEN = 5*1024*1024
	FILE_BLOCK_SIZE			 = 5*1024*1024
)

const (
	TASK_STOP 		= 1
	TASK_READ_BLOCK	= 2
	TASK_COMBINE_T1	= 3
	TASK_PAUSE_CLEAN	= 4
	TASK_RESUME_CLEAN	= 5
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
	modifyBuffer *cacheBuffer
	buffAllocMtx sync.Mutex
	io *S3FileIO
	
	fileName 		string
	openFlags		uint32
	fileLen 		int64
	filePersisentLen int64
	
	t1BlockNumStart int64
	t1BlockNumEnd 	int64
	t2BlockNumStart	int64
	t2BlockNumEnd	int64
	
	//for block maintain task
	mtTaskStarted 	bool
	mtChan			(chan taskCmd)
	
	modified		bool 			//the file had been modified
	modBlockList 	*list.List		//modified block list
	cacheCleanMtx   sync.Mutex
} 

func newRemoteCache(name string, length int64, io *S3FileIO) *remoteCache {
	return &remoteCache{
		io : io,
		fileName: name,
		fileLen: length,
		
		appendBuffer    : newCacheBuffer(),
		t1BlockNumStart	: 0,
		t1BlockNumEnd	: 0,
		t2BlockNumStart : 0,
		t2BlockNumEnd   : 0,
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

func (me *remoteCache) Write(data []byte, offset int64) int {
	if me.mtTaskStarted == false {
		go me.blockMaintainTask()
		me.mtTaskStarted = true
	}
	
	count := len(data)
	if count > 0 {
		me.modified = true
	}
	
	//append case
	if offset == me.fileLen {
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

func (me *remoteCache) blockMaintainTask() {
	var task taskCmd
	var rc int
	for true {
		task = <- me.mtChan	//wait for command
		switch task.cmd {			
			case TASK_STOP:
				break
			case TASK_READ_BLOCK:
				rc = me.readBlock(task.data, task.blknum)
				task.waitChan <- rc
				break
			case TASK_COMBINE_T1:
				//verify it again and then start combine process
				if me.t1BlockNumEnd - me.t1BlockNumStart >= 1024 {
					me.combineT1Blocks(me.t1BlockNumStart, 1024)
				}
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

//combine tier 1 blocks
func (me *remoteCache) combineT1Blocks(start int64, count int) int {
	//start multipart upload
	tgtName := fmt.Sprintf("$cache$%s.T2_%d", me.fileName, me.t2BlockNumEnd)
	uploadId,ok := me.io.startUpload(tgtName)
	if ok !=0 {
		log.Println("Failed to start upload for file ", tgtName)
		return ok
	}
	
	//multipart copy
	n := 1
	plist := make([]*s3.CompletedPart, 1024)
	for i:=start; i<start+int64(count); i++ {
		srcName := fmt.Sprintf("$cache$%s.T1_%d", me.fileName, i)
		cp,ok := me.io.copyPart(tgtName, srcName, uploadId, int64(n))
		if ok != 0 {
			log.Printf("Failed to copy part %d from %s to %s\n", n, srcName, tgtName)
			return ok
		}
		plist = append(plist, cp)
		n++
	}
	
	//complete multipart upload
	ok = me.io.completeUpload(tgtName, uploadId, plist)
	if ok != 0 {
		log.Printf("Failed to complete copy for file %s, upload id %s\n", tgtName, uploadId)
	}
	//need sync here
	me.t2BlockNumEnd += 1
	me.t1BlockNumStart += int64(count)
	return ok
}

//combine tier 2 blocks
func (me *remoteCache) combineT2Blocks(start int64, count int) int {
	//start multipart upload
	tgtName := me.fileName
	uploadId,ok := me.io.startUpload(tgtName)
	if ok !=0 {
		log.Println("Failed to start upload for file ", tgtName)
		return ok
	}
	
	//multipart copy
	n := 1
	plist := make([]*s3.CompletedPart, 1024)
	for i:=start; i<start+int64(count); i++ {
		srcName := fmt.Sprintf("$cache$%s.T2_%d", me.fileName, i)
		cp,ok := me.io.copyPart(tgtName, srcName, uploadId, int64(n))
		if ok != 0 {
			log.Printf("Failed to copy part %d from %s to %s\n", n, srcName, tgtName)
			return ok
		}
		plist = append(plist, cp)
		n++
	}
	
	//complete multipart upload
	ok = me.io.completeUpload(tgtName, uploadId, plist)
	if ok != 0 {
		log.Printf("Failed to complete copy for file %s, upload id %s\n", tgtName, uploadId)
	}
	//need sync here
	me.t2BlockNumEnd += int64(count)

	return ok
}

func (me *remoteCache) appendFile(data []byte, offset int64) int {
	//allocate append buffer if not yet
	if me.appendBuffer == nil {
		me.buffAllocMtx.Lock()
		if me.appendBuffer == nil {
			me.appendBuffer = newCacheBuffer()
		}
		me.buffAllocMtx.Unlock()
	}
	
	remainN := len(data)
	
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
		copy(buff, data[0:copyN])
		
		me.appendBuffer.dataLen += copyN
		remainN -= copyN
		
		//buffer is full, trigger T1 block uploading
		if me.appendBuffer.dataLen == me.appendBuffer.bufferLen {
			//verify if me.t1BlockNumEnd is correct
			num := ((offset + FILE_BLOCK_SIZE) / FILE_BLOCK_SIZE)
			if num != me.t1BlockNumEnd {
				log.Println("There must be something wrong")
			}
			
			//upload the buffer
			name := fmt.Sprintf("$cache$%s.T1_%d", me.fileName, me.t1BlockNumEnd)
			ok := me.io.putBlock(name, me.appendBuffer.buffer)
			if ok != 0 {		//we cannot move forward if buffer cannot be uploaded
				log.Println("Failed to upload buffer to file ", name)
				return ok
			}
			me.t1BlockNumEnd += 1
			me.appendBuffer.dataLen = 0
		}
	}	
	
	//the number of tier 1 block reach uplimit, start the background combination thread
	if me.t1BlockNumEnd - me.t1BlockNumStart >= 1024 {
		//go me.combineT1Blocks(me.t1BlockNumStart, 1024)
		//send end number to "file block maintain task"
		select {
			case me.mtChan <- taskCmd{cmd: TASK_COMBINE_T1, blknum: me.t1BlockNumEnd}:	
			default:
		}
	}
	
	//increase file size
	me.fileLen += int64(len(data))
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

func (me *remoteCache) readBlock(dest []byte, blknum int64)(int) {
	return 0
}

//"offset" is base on start point of cache
func (me *remoteCache) readAppendCache(dest []byte, offset int64)(int) {
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
			name := fmt.Sprintf("$cache$%s.T1_%d", me.fileName, blknum)
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
			name := fmt.Sprintf("$cache$%s.T2_%d", me.fileName, blknum)
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
}

func (me *remoteCache) Read(dest []byte, offset int64)(int) {
	//never modified? simply read data from remote
	if me.modified == false {
		return me.io.getBuffer(me.fileName, dest, offset)
	}
	
	//file was modified, but the scope to be accessed is not modified
	if offset + int64(len(dest)) < me.filePersisentLen {
		return me.io.getBuffer(me.fileName, dest, offset)
	}
	
	//part of data is in original file, part of data is in append buffer
	//read the first part
	var readLen int = 0
	var count int = len(dest)
	if offset < me.filePersisentLen {
		n := int(me.filePersisentLen - offset)
		rc := me.io.getBuffer(me.fileName, dest[:n], offset)
		if rc != int(n) {
			return rc
		}
		count -= n
		offset += int64(n)
		readLen += n
	}
	
	//read append buffer now
	//new offset base on append cache start
	offset -= me.filePersisentLen
	//stop any cache clean operation, wait here until it's received
	me.cacheCleanMtx.Lock()
	n := me.readAppendCache(dest[readLen:], offset)
	me.cacheCleanMtx.Unlock()
	if n < 0 {
		return n
	}else{
		return readLen + n
	}
	
	//unkown case
	log.Println("Run into unkown case.")
	return -1
}

func (me *remoteCache) Flush()int {
	return 0
}
