package s3impl

import (
	"fmt"
	"log"
	//"encoding/json"

	"github.com/allspace/csmgr/common"
	"github.com/aws/aws-sdk-go/service/s3"
)

type sliceMeta struct {
	SliceSize        int64
	SliceCount       int64
	CurSliceFileName string
	CurSliceFileLen  int64
	FileLen          int64 //file length, include committed blocks only
}

type sliceFile struct {
	fscommon.SliceFile
	io *S3FileIO

	FileName     string
	metaFileName string
	meta         sliceMeta
	isSlicedFile bool
}

func NewSliceFile(io fscommon.FileIO) fscommon.ISliceFile {
	return &sliceFile{io: io.(*S3FileIO)}
}

//append a block to slice file
func (me *sliceFile) Append(blocks []int64, data []byte) int {

	appendLen := int64(len(blocks)*FILE_BLOCK_SIZE) + int64(len(data))

	//special case #1: append to zero current slice and total size is less than a slice
	//no need to use temp file in this case
	if me.meta.CurSliceFileLen == 0 && appendLen < me.meta.SliceSize {
		_, ok := me.mergeBlocksAndBuffer(me.meta.CurSliceFileName, "", 0, blocks, data)
		if ok < 0 {
			log.Println("Failed to append data for ", me.FileName)
			return ok
		}
		me.meta.CurSliceFileLen += appendLen
		me.meta.FileLen += appendLen
		me.SaveMeta()
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
		me.SaveMeta()
		return 0
	}

	//general cases, include:
	//1. current slice is zero, but the data to append is more than one slice
	//2. current slice is less than S3_MIN_BLOCK_SIZE, there is at least one block to append
	//3. current slice >= S3_MIN_BLOCK_SIZE, there is either blocks or buffer (or both) to append
	//They all need use temp file
	tmpFile := fmt.Sprintf("$tmp$/%s.tmp2", me.FileName)

	tmpLen, ok := me.mergeBlocksAndBuffer(tmpFile,
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
		me.meta.FileLen += (me.meta.SliceSize - me.meta.CurSliceFileLen)
		me.meta.CurSliceFileLen = 0
		me.SaveMeta()
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
	me.SaveMeta()

	//remove temp file
	me.io.Unlink(tmpFile)
	return 0

	//save meta data file
	//if the fs crash before there is a chance to save meta data, at recovery time,
	//we have to discard the current slice file, and all data after that
	//this is because we don't know if the data in current slice is valid or not
	//no worry it's successful or not at this time. it will be saved at flush, and be recovred at open
}

func (me *sliceFile) catBuffer2SmallSlice(rt string, rtLen int64, data []byte) int {
	//remote file is zero length
	//it may also mean that remote file does not contains valid data
	if rtLen == 0 {

		return me.io.PutBuffer(rt, data)

	} else if rtLen < S3_MIN_BLOCK_SIZE { //remote file > 0, but < S3_MIN_BLOCK_SIZE

		tmpBuff := make([]byte, int(rtLen)+len(data))
		n := me.io.GetBuffer(rt, tmpBuff, 0)
		if n != int(rtLen) {
			log.Printf("Something got wrong: rtLen = %d, n = %d\n", rtLen, n)
			return fscommon.EIO
		}
		copy(tmpBuff[n:], data)
		return me.io.PutBuffer(rt, tmpBuff)

	}

	return -1
}

func (me *sliceFile) combineRemote(tgt string, file1 string, file1Len int64, file2 string, file2Len int64) int {
	tmpBuff := make([]byte, file1Len+file2Len)
	//download file1
	var n int = 0
	if file1Len > 0 {
		n = me.io.GetBuffer(file1, tmpBuff, 0)
	} else {
		n = 0 //for case original file does not exist, or its size is zero
	}

	//download file2
	var m int = 0
	m = me.io.GetBuffer(file2, tmpBuff[n:], 0)

	//upload the combined version
	ok := me.io.PutBuffer(tgt, tmpBuff[0:n+m])

	tmpBuff = nil
	return ok
}

//need support these cases:
//1. slice file > 0 and < S3_MIN_BLOCK_SIZE, there is at least one block to append
//2. slice file >= S3_MIN_BLOCK_SIZE
//this method does not affect meta data
func (me *sliceFile) mergeBlocksAndBuffer(tgt string, rt string, rtLen int64, blocks []int64, data []byte) (int64, int) {
	log.Printf("mergeBlocksAndBuffer is called: rtLen=%d, blocks=%d, data len=%d\n", rtLen, len(blocks), len(data))

	tmpFile := ""

	if rtLen == 0 && len(blocks) == 0 {
		ok := me.io.PutBuffer(tgt, data)
		if ok < 0 {
			return 0, ok
		} else {
			return int64(ok), 0
		}
	}

	if rtLen > 0 && rtLen < S3_MIN_BLOCK_SIZE {
		if len(blocks) > 0 {
			tmpFile := fmt.Sprintf("$tmp$/%s.tmp3", rt)
			file := me.GetCacheBlockFileName(blocks[0])
			tmpLen := me.combineRemote(tmpFile, rt, rtLen, file, FILE_BLOCK_SIZE)
			rt = tmpFile
			rtLen = int64(tmpLen)
			blocks = blocks[1:]
		}
	}

	var totalLen int64 = 0

	plist := make([]*s3.CompletedPart, 0)

	uploadId, ok := me.io.startUpload(tgt)
	if ok < 0 {
		return 0, ok
	}

	//copy remote file if there is
	var pnum int64 = 1
	if rtLen > 0 {
		cps, pnum := me.io.copyBigPart(tgt, rt, rtLen, uploadId, pnum)
		if pnum < 0 {
			return 0, int(pnum)
		}
		copy(plist, cps)
		totalLen += rtLen
	}

	//copy remote blocks if there is
	var cp *s3.CompletedPart = nil
	if len(blocks) > 0 {
		for _, blkId := range blocks {
			if blkId < 0 { //it's possible the list contains empty entries which means they have beem consumed
				continue
			}
			if blkId < me.meta.FileLen { //we don't support random write, or we should discard outdated blocks
				continue
			}
			file := me.GetCacheBlockFileName(blkId)
			cp, ok = me.io.copyPart(tgt, file, "", uploadId, pnum)
			if ok < 0 {
				return 0, ok
			}
			plist = append(plist, cp)
			pnum++
			totalLen += FILE_BLOCK_SIZE
		}
	}

	//upload local buffer
	if len(data) > 0 {
		cp, ok = me.io.uploadPart(tgt, data, uploadId, pnum)
		if ok < 0 {
			me.io.cleanMultipartUpload(tgt, uploadId)
			return 0, ok
		}
		plist = append(plist, cp)
		totalLen += int64(len(data))
	}

	//complete multipart upload
	ok = me.io.completeUpload(tgt, uploadId, plist)
	if ok < 0 {
		me.io.cleanMultipartUpload(tgt, uploadId)
		return 0, ok
	}

	if len(tmpFile) > 0 {
		me.io.Unlink(tmpFile)
	}

	return totalLen, 0
}
