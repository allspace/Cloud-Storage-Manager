package s3impl

import (
	"fmt"
	"log"
	"encoding/json"
	
	"github.com/aws/aws-sdk-go/service/s3"
	"brightlib.com/common"
)

type sliceMeta struct {
	SliceSize  int64
	SliceCount int64
	CurSliceFileName string
	CurSliceFileLen int64
	FileLen	int64			//file length, include committed blocks only
}

type sliceFile struct {
	io *S3FileIO
	
	FileName string
	metaFileName string
	meta sliceMeta
	isSlicedFile bool
}

func (me *sliceMeta) AppendLength(aLen int64) {
	me.CurSliceFileLen += aLen
	me.FileLen += aLen
}

func (me *sliceMeta) IncSliceCount(count int) {
	me.SliceCount += int64(count)
}

func (me *sliceMeta) SetSliceCount(count int64) {
	me.SliceCount = count
}


func NewSliceFile(io *S3FileIO) *sliceFile{
	return &sliceFile{ io : io}
}

func (me *sliceFile) getCacheBlockFileName(blkId int64) string {
	return fmt.Sprintf("/$cache$/%s/blocks/%d", me.FileName, blkId)
}

func (me *sliceFile) GetLength() int64 {
	return me.meta.FileLen
}

func (me *sliceFile) Open(path string, flags uint32) int {
	me.FileName     = path
	me.metaFileName = fmt.Sprintf("/$slice$/%s/meta", path)
	
	ok := me.tryRecovery(path)
	if ok < 0 {
		log.Println("Unable recover file ", path)
		return ok
	}
	
	if me.isSlicedFile == false {
		me.meta.CurSliceFileName = me.FileName
		rc := me.io.GetRemoteFileSize(me.meta.CurSliceFileName)
		if rc < 0 {
			if rc != fscommon.ENOENT {
				return int(rc)
			} else {
				if ((flags&fscommon.O_CREAT)==0) {
					return fscommon.ENOENT
				}else{
					rc := me.io.putFile(path, nil)
					if rc < 0 {
						return rc
					}
				}
				rc = 0
			}
		}
		me.meta.CurSliceFileLen = rc
		me.meta.FileLen = rc
		me.meta.SliceSize = FILE_SLICE_SIZE
	}

	return 0
}

//it's possible there is data inconsistency (e.g. fs crash) during last mount
//in that case we need do some recovery now
func (me *sliceFile) tryRecovery(path string) int {
	var needUpdate bool = false
	
	//check if path is a slice file by check its meta data
	ok := me.loadMeta()

	//before extend a normal file to slice file, we will "touch" a meta data file first.
	//this guarantees meta data file always exists, even it's not updated
	//no meta data file, not a slice file, no need recover
	if ok == fscommon.ENOENT {
		me.isSlicedFile = false
		log.Printf("%s is not a sliced file.\n", path)
		return 0
	}
	//other failure, cannot go further
	if ok < 0 {
		return ok
	}
	
	//at this step, we are sure this is a sliced file
	me.isSlicedFile = true
	
	if me.meta.CurSliceFileName != path {
		log.Printf("Slice meta data mismatch: file name = %s, current slice name = %s\n", path, me.meta.CurSliceFileName)
		return -1
	}
	
	//check if me.meta.CurSliceFileLen is updated
	size := me.io.GetRemoteFileSize(me.meta.CurSliceFileName)
	if size != me.meta.CurSliceFileLen {
		me.meta.AppendLength(size - me.meta.CurSliceFileLen)
		needUpdate = true
	}
	//check if SliceCount/FileLen is updated
	//load slice list first
	dir := fmt.Sprintf("$slice$/%s", path)
	dis,ok := me.io.ListDir(dir)
	if ok < 0 {
		return ok
	}
	var count int64 = 0
	for _,di := range dis {
		if di.Name == "meta" {
			continue
		}
		if int64(di.Size) != me.meta.SliceSize {
			log.Printf("Mismatched file slice size: file %s's size: %d, expected size: %d", di.Name, di.Size, me.meta.SliceSize)
			return -1
		}
		count++
	}
	
	//this means meta data was not updated after a new formal slice was created
	if me.meta.SliceCount != count {
		me.meta.SetSliceCount(count)
		needUpdate = true
	}
	
	size = count * me.meta.SliceSize + me.meta.CurSliceFileLen
	if size > me.meta.FileLen {
		//we are sure that all formal slices are valid, but we are not sure if current slice is valid
		//we have to discard it
		me.meta.FileLen = count * me.meta.SliceSize
		ok = me.io.zeroFile(me.meta.CurSliceFileName)
		if ok != 0 {
			return ok
		}
		needUpdate = true
	}else if size < me.meta.FileLen {
		//don't why, but let's just update correct value
		me.meta.FileLen = size
		needUpdate = true
	}
	
	//save meta data file if something got corrected
	if needUpdate {
		return me.saveMeta()
	}
	
	return 0
}

//load slice file meta data
//if there is only one slice (normal file), there won't be meta data file
func (me *sliceFile) loadMeta() int {
	data := make([]byte, 1024)
	ok := me.io.getBuffer(me.metaFileName, data, 0)
	if ok <= 2 {
		return ok
	}
	if data[0]!='\x01' || data[1]!='\x00' {
		return -1
	}
	err := json.Unmarshal(data[2:], me.meta)
	if err != nil {
		log.Println("error: ", err)
		return -1
	}
	return 0
}

//save slice file meta data
func (me *sliceFile) saveMeta() int {
	//no need meta data file when no full slice
	if me.meta.SliceCount == 0 {
		return 0
	}
	
	b, err := json.Marshal(me.meta)
	if err != nil {
		log.Println("error:", err)
		return -1
	}
	data := make([]byte, len(b)+2)
	copy(data[2:], b)
	data[0] = '\x01'
	data[1] = '\x00'
	ok := me.io.putFile(me.metaFileName, data)
	return ok
}

func (me *sliceFile) Read(data []byte, offset int64)int {
	if offset >= me.meta.FileLen {
		return 0	//EOF
	}
	
	//case #1: there is no addtional full slice
	if me.meta.SliceCount == 0 {
		return me.io.getBuffer(me.FileName, data, offset)
	}
	
	//case #2: there is at least one full slice
	sliceNum := int64(offset / me.meta.SliceSize)
	sliceOffset := offset % me.meta.SliceSize
	reqLen := len(data)
	var n int64 = 0
	var rc int = 0
	
	if sliceOffset + int64(reqLen) <= me.meta.SliceSize {
		n = int64(reqLen)
	}else{
		n = sliceOffset + int64(reqLen) - me.meta.SliceSize
	}
	
	//the first part
	name := fmt.Sprintf("$slice$/%s/files/%d.dat", me.FileName, sliceNum)
	rc = me.io.getBuffer(name, data[0:n], sliceOffset)
	if n == int64(reqLen) {
		return rc
	}
	
	//no more slice
	if sliceNum >= me.meta.SliceCount {
		return rc
	}
	
	//the second part falls into next slice
	sliceNum++
	n = int64(reqLen) - n
	name = fmt.Sprintf("$slice$/%s/files/%d.dat", me.FileName, sliceNum)
	rc = me.io.getBuffer(name, data[n:], 0)
	if rc == int(n) {
		return reqLen
	}else if rc >= 0 && rc < int(n) {
		return reqLen - (int(n) - rc)
	}else{
		return rc
	}
	
	return 0
}

//append a block to slice file
func (me *sliceFile) Append(blocks []int64, data []byte) int{

	appendLen := int64(len(blocks) * FILE_BLOCK_SIZE) + int64(len(data))
	
	//special case #1: append to zero current slice and total size is less than a slice
	//no need to use temp file in this case
	if me.meta.CurSliceFileLen == 0 && appendLen < me.meta.SliceSize {
		_,ok := me.mergeBlocksAndBuffer(me.meta.CurSliceFileName, "", 0, blocks, data)
		if ok < 0 {
			log.Println("Failed to append data for ", me.FileName)
			return ok
		}
		me.meta.CurSliceFileLen += appendLen
		me.meta.FileLen += appendLen
		me.saveMeta()
		return 0
	}
	
	//special case #2: current slice is less than S3_MIN_BLOCK_SIZE, and there is only a buffer to append
	//no need to use temp file in this case (but needs a big buffer)
	//if FILE_BLOCK_SIZE==5MB, it generally means file less than 10MB
	if me.meta.CurSliceFileLen > 0 && me.meta.CurSliceFileLen < S3_MIN_BLOCK_SIZE {
		ok := me.catBuffer2SmallSlice(me.meta.CurSliceFileName, me.meta.CurSliceFileLen, data)
		if ok < 0 {
			return ok
		}
		me.meta.CurSliceFileLen += appendLen
		me.meta.FileLen += appendLen
		me.saveMeta()
		return 0
	}
	
	//general cases, include:
	//1. current slice is zero, but the data to append is more than one slice
	//2. current slice is less than S3_MIN_BLOCK_SIZE, there is at least one block to append
	//3. current slice >= S3_MIN_BLOCK_SIZE, there is either blocks or buffer (or both) to append
	//They all need use temp file
	tmpFile := fmt.Sprintf("$tmp$/%s.tmp2", me.FileName)
	
	tmpLen,ok := me.mergeBlocksAndBuffer(tmpFile, 
											me.meta.CurSliceFileName, 
											me.meta.CurSliceFileLen, 
											blocks, data)
	if ok < 0 {
		return ok
	}
		
	//need create a new slice
	if tmpLen >= me.meta.SliceSize {
		sliceFileName := fmt.Sprintf("$slice$/%s/files/%d.dat", me.meta.SliceCount)
		byteRange := fmt.Sprintf("bytes=0-%d", me.meta.SliceSize)
		ok = me.io.copyFileByRange(sliceFileName, tmpFile, byteRange)
		if ok < 0 {
			return ok
		}
		
		me.meta.SliceCount++		
		me.meta.FileLen += (me.meta.SliceSize-me.meta.CurSliceFileLen)
		me.meta.CurSliceFileLen = 0
		me.saveMeta()
		tmpLen -= me.meta.SliceSize
	}
	
	//update current slice
	byteRange := fmt.Sprintf("bytes=%d-", me.meta.SliceSize)
	ok = me.io.copyFileByRange(me.meta.CurSliceFileName, tmpFile, byteRange)
	if ok < 0 {
		return ok
	}
	
	//update meta data
	me.meta.CurSliceFileLen = tmpLen
	me.meta.FileLen += (tmpLen - me.meta.CurSliceFileLen)
	me.saveMeta()
	
	//remove temp file
	me.io.Unlink(tmpFile)
	return 0
	
	//save meta data file
	//if the fs crash before there is a chance to save meta data, at recovery time, 
	//we have to discard the current slice file, and all data after that
	//this is because we don't know if the data in current slice is valid or not
	//no worry it's successful or not at this time. it will be saved at flush, and be recovred at open
}

func (me *sliceFile) Truncate(size uint64)int {
	if size == 0 {
		ok := me.io.putFile(me.FileName, nil)
		if ok < 0 {
			return ok
		}
		me.meta.FileLen = 0		
		me.meta.CurSliceFileLen = 0
		me.meta.SliceCount = 0
		
		//TODO: unlink all slice files
		me.io.Unlink(me.metaFileName)
	}
	return 0
}

func (me *sliceFile) catBuffer2SmallSlice(rt string, rtLen int64, data []byte)int {	
	//remote file is zero length
	//it may also mean that remote file does not contains valid data
	if rtLen == 0 {		
	
		return me.io.putFile(rt, data)
		
	} else if rtLen < S3_MIN_BLOCK_SIZE {  //remote file > 0, but < S3_MIN_BLOCK_SIZE
	
		tmpBuff := make([]byte, int(rtLen) + len(data))
		n := me.io.getBuffer(rt, tmpBuff, 0)
		if n != int(rtLen) {
			log.Printf("Something got wrong: rtLen = %d, n = %d\n", rtLen, n)
			return fscommon.EIO
		}
		copy(tmpBuff[n:], data)
		return me.io.putFile(rt, tmpBuff)
		
	} 
	
	return -1
}

func (me *sliceFile) combineRemote(tgt string, file1 string, file1Len int64, file2 string, file2Len int64)int {
	tmpBuff := make([]byte, file1Len + file2Len)
	//download file1
	var n int = 0
	if file1Len > 0 {
		n = me.io.getBuffer(file1, tmpBuff, 0)
	}else{
		n = 0		//for case original file does not exist, or its size is zero
	}
	
	//download file2
	var m int = 0
	m = me.io.getBuffer(file2, tmpBuff[n:], 0)
	
	//upload the combined version
	ok := me.io.putFile(tgt, tmpBuff[0:n+m])
	
	tmpBuff = nil
	return ok
}

//need support these cases:
//1. slice file > 0 and < S3_MIN_BLOCK_SIZE, there is at least one block to append
//2. slice file >= S3_MIN_BLOCK_SIZE
//this method does not affect meta data
func (me *sliceFile) mergeBlocksAndBuffer(tgt string, rt string, rtLen int64, blocks []int64, data []byte)(int64, int) {
	log.Printf("mergeBlocksAndBuffer is called: rtLen=%d, blocks=%d, data len=%d\n", rtLen, len(blocks), len(data))
	
	tmpFile := ""
	
	if rtLen == 0 && len(blocks) == 0 {
		ok := me.io.putFile(tgt, data)
		if ok < 0 {
			return 0, ok
		}else{
			return int64(ok), 0
		}
	}
	
	if rtLen > 0 && rtLen < S3_MIN_BLOCK_SIZE {
		if len(blocks) > 0 {
			tmpFile := fmt.Sprintf("$tmp$/%s.tmp3", rt)
			file := me.getCacheBlockFileName(blocks[0])
			tmpLen := me.combineRemote(tmpFile, rt, rtLen, file, FILE_BLOCK_SIZE)
			rt = tmpFile
			rtLen = int64(tmpLen)
			blocks = blocks[1:]
		}
	}
	
	var totalLen int64 = 0

	plist := make([]*s3.CompletedPart, 0)
	
	uploadId,ok := me.io.startUpload(tgt)
	if ok < 0 {
		return 0,ok
	}
	
	//copy remote file if there is
	var pnum int64 = 1
	if rtLen > 0 {
		cps, pnum := me.io.copyBigPart(tgt, rt, rtLen, uploadId, pnum)
		if pnum < 0 {
			return 0,int(pnum)
		}
		copy(plist, cps)
		totalLen += rtLen
	}
	
	//copy remote blocks if there is
	var cp *s3.CompletedPart = nil
	if len(blocks) > 0 {
		for _,blkId := range blocks {
			if blkId < 0 {		//it's possible the list contains empty entries which means they have beem consumed
				continue
			}
			if blkId < me.meta.FileLen {	//we don't support random write, or we should discard outdated blocks
				continue
			}
			file := me.getCacheBlockFileName(blkId)
			cp,ok = me.io.copyPart(tgt, file, "", uploadId, pnum)
			if ok < 0 {
				return 0,ok
			}
			plist = append(plist, cp)
			pnum++
			totalLen += FILE_BLOCK_SIZE
		}
	}
	
	//upload local buffer
	if len(data) > 0 {
		cp,ok = me.io.uploadPart(tgt, data, uploadId, pnum)
		if ok < 0 {
			me.io.cleanMultipartUpload(tgt, uploadId)
			return 0,ok
		}
		plist = append(plist, cp)
		totalLen += int64(len(data))
	}
	
	//complete multipart upload
	ok = me.io.completeUpload(tgt, uploadId, plist)
	if ok < 0 {
		me.io.cleanMultipartUpload(tgt, uploadId)
		return 0,ok
	}
	
	if len(tmpFile) > 0 {
		me.io.Unlink(tmpFile)
	}
	
	return totalLen,0
}

