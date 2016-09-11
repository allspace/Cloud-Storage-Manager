package s3impl
import (
	"fmt"
	"log"
	"time"
	"sync"
	"container/list"
	
	"github.com/aws/aws-sdk-go/service/s3"
	"brightlib.com/common"
)

const(
	DEFAULT_CACHE_BUFFER_LEN = 5*1024*1024
	FILE_BLOCK_SIZE			 = 5*1024*1024
	T2_BLOCK_SIZE			 = 5*1024*1024*1024
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

//write function
func (me *remoteCache) Write(data []byte, offset int64) int {
	log.Println("Write is called for file ", me.fileName)
	
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

func (me *remoteCache) Close()int {
	log.Printf("Close is called for file %s\n", me.fileName)
	
	//readonly
	if me.modified != true {
		return 0
	}
	
	//try to upload last part (which is in buffer) of the file
	//if total file length is less than FILE_BLOCK_SIZE, then this is the file
	//me.fileLen FILE_BLOCK_SIZE
	fsize := me.io.GetRemoteFileSize(me.fileName)
	if fsize < 0 {
		if fsize != fscommon.ENOENT {
			log.Println("Failed to get size for file ", me.fileName)
			return int(fsize)	//may be an EIO error
		}		
	}
	
	return me.combineFileForAppend(me.fileName, fsize)
	
	//run parts combination
	//tc := taskCmd{cmd: TASK_COMBINE_T2, waitChan: make(chan int)}
	//me.mtChan <- tc
	//rs := <- tc.waitChan
	//return rs
}

func (me *remoteCache) Flush()int {
	return 0
}

func (me *remoteCache) Utimens(Mtime *time.Time)(int) {
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
				rc = me.readBlock(task.data, task.blknum)
				task.waitChan <- rc
				break
			case TASK_COMBINE_T1:
				//verify it again and then start combine process
				for me.t1BlockNumEnd - me.t1BlockNumStart >= 1024 {
					me.combineT1Blocks(me.t1BlockNumStart, 1024)
				}
				break
			case TASK_COMBINE_T2:
				for me.t1BlockNumEnd - me.t1BlockNumStart > 0 {
					var count int = 0
					if me.t1BlockNumEnd - me.t1BlockNumStart > 1024 {
						count = 1024
					}else{
						count = int(me.t1BlockNumEnd - me.t1BlockNumStart)
					}
					me.combineT1Blocks(me.t1BlockNumStart, count)
				}
				rc := me.combineT2Blocks(me.t2BlockNumStart, int(me.t2BlockNumEnd-me.t2BlockNumStart))
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

//combine tier 1 blocks
func (me *remoteCache) combineT1Blocks(start int64, count int) int {
	//start multipart upload
	tgtName := fmt.Sprintf("$cache$/%s.T2_%d", me.fileName, me.t2BlockNumEnd)
	uploadId,ok := me.io.startUpload(tgtName)
	if ok !=0 {
		log.Println("Failed to start upload for file ", tgtName)
		return ok
	}
	
	//multipart copy
	n := 1
	plist := make([]*s3.CompletedPart, 1024)
	for i:=start; i<start+int64(count); i++ {
		srcName := fmt.Sprintf("$cache$/%s.T1_%d", me.fileName, i)
		cp,ok := me.io.copyPart(tgtName, srcName, "", uploadId, int64(n))
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
	
	me.io.WaitFileReady(tgtName)
	
	//need sync here
	me.t2BlockNumEnd += 1
	me.t1BlockNumStart += int64(count)
	return ok
}

//combine tier 2 blocks
func (me *remoteCache) combineT2Blocks(start int64, count int) int {
	fileLen := me.io.GetRemoteFileSize(me.fileName)
	if fileLen < 0 {
		return -1
	}
	
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
	
	//copy original file
	var rangeStart int64 = 0
	for rangeStart < fileLen {
		byteRange := fmt.Sprintf("bytes=%d-%d", rangeStart, rangeStart + T2_BLOCK_SIZE)
		cp,ok := me.io.copyPart(tgtName, me.fileName, byteRange, uploadId, int64(n))
		if ok != 0 {
			log.Printf("Failed to copy part %d from %s to %s\n", n, me.fileName, tgtName)
			return ok
		}
		plist = append(plist, cp)
		n++
		
		rangeStart += T2_BLOCK_SIZE
	}
	
	//copy new created T2 blocks
	for i:=start; i<start+int64(count); i++ {
		srcName := fmt.Sprintf("$cache$/%s.T2_%d", me.fileName, i)
		cp,ok := me.io.copyPart(tgtName, srcName, "", uploadId, int64(n))
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
	
	me.io.WaitFileReady(tgtName)
	
	//need sync here
	me.t2BlockNumEnd += int64(count)

	return ok
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
		me.totalAppendSize += int64(copyN)
		
		//buffer is full, trigger T1 block uploading
		if me.appendBuffer.dataLen == me.appendBuffer.bufferLen {
			//verify if me.t1BlockNumEnd is correct
			num := ((offset + FILE_BLOCK_SIZE) / FILE_BLOCK_SIZE)
			if num != me.appendBlockNumEnd {
				log.Println("There must be something wrong")
			}
			
			//upload the buffer
			name := fmt.Sprintf("$cache$/%s.A1_%d", me.fileName, me.appendBlockNumEnd)
			ok := me.io.putBlock(name, me.appendBuffer.buffer)
			if ok != 0 {		//we cannot move forward if buffer cannot be uploaded
				log.Println("Failed to upload buffer to file ", name)
				return ok
			}
			me.appendBlockNumEnd += 1
			me.appendBuffer.dataLen = 0
		}
	}	
	
	//the number of tier 1 block reach uplimit, start the background combination thread
	if me.appendBlockNumEnd - me.appendBlockNumStart >= 1024 {
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

//upload last part of the file
//it may be the whole file itself
func (me *remoteCache) uploadLastPart(name string)(int) {
	if me.appendBuffer.dataLen == 0 {
		return 0
	}
	
	if len(name) == 0 {
		name = fmt.Sprintf("$cache$/%s.T1_%d", me.fileName, me.t1BlockNumEnd)
	}
	
	ok := me.io.putBlock(name, me.appendBuffer.buffer[0:me.appendBuffer.dataLen])
	if ok < 0 {		//we cannot move forward if buffer cannot be uploaded
		log.Println("Failed to upload last part for file ", name)
		return ok
	}
	return 0
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
}

func (me *remoteCache) combineFileForAppend(oriFile string, fsize int64)(int) {	
	log.Printf("combineFileForAppend is called for file %s, fsize=%d", oriFile, fsize)
	var ok int
	
	//original file size is less than 5MB, so we cannot use multipart copy here
	if fsize < FILE_BLOCK_SIZE {
		
		//special case: only one block in append buffer, and original file does not exist, or its size is zero
		if (fsize <= 0) && (me.appendBlockNumEnd == me.appendBlockNumStart) {
			ok = me.io.putBlock(oriFile, me.appendBuffer.buffer[0:me.appendBuffer.dataLen])
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
		ok = me.io.putBlock(oriFile, tmpBuff[0:n+m])
		if ok < 0 {
			return ok
		}
		
		//no additional blocks, we are ready to return
		if me.appendBlockNumEnd == 0 {
			return 0
		}
	}
	
	//a multipart copy process
	tgt := fmt.Sprintf("$cache$/%s_tmp")
	uploadId,ok := me.io.startUpload(tgt)
	if ok < 0 {
		return ok
	}
	plist := make([]*s3.CompletedPart, 2)
	
	//oeiginal file is the 1st part of this multipart copy process
	part, ok := me.io.copyPart(tgt, oriFile, "", uploadId, 1)
	plist = append(plist, part)
	
	var pnum int64 = 2
	startNum := me.appendBlockNumStart - 1
	for i:=startNum; i< me.appendBlockNumEnd; i++ {
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


