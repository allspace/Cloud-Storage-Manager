package s3impl

import (
	"os"
	//"syscall"
	"bytes"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	//"github.com/aws/aws-sdk-go/aws/session"
	"github.com/allspace/csmgr/common"
	"github.com/aws/aws-sdk-go/service/s3"
)

const (
	S3_MAX_PART_SIZE = 5 * 1024 * 1024 * 1024
)

type S3FileIO struct {
	svc        *s3.S3
	fs         *S3FileSystemImpl
	bucketName string
}

func (me *S3FileIO) startUpload(name string) (string, int) {

	params := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(me.bucketName), // Required
		Key:    aws.String(name),          // Required
	}
	rsp, err := me.svc.CreateMultipartUpload(params)
	if err != nil {
		log.Println(err.Error())
		return "", fscommon.EIO
	}

	return *rsp.UploadId, 0
}

func (me *S3FileIO) completeUpload(name string, uploadId string, plist []*s3.CompletedPart) int {
	params := &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(me.bucketName), // Required
		Key:      aws.String(name),          // Required
		UploadId: aws.String(uploadId),      // Required
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: plist,
		},
	}
	_, err := me.svc.CompleteMultipartUpload(params)
	if err != nil {
		log.Println(err.Error())
		return fscommon.EIO
	}

	return 0
}

func (me *S3FileIO) copyPart(tgtName string, srcName string, byteRange string, uploadId string, pnum int64) (*s3.CompletedPart, int) {
	var copySrc string
	if srcName[0] == '/' {
		copySrc = "/" + me.bucketName + srcName
	} else {
		copySrc = "/" + me.bucketName + "/" + srcName
	}
	params := &s3.UploadPartCopyInput{
		Bucket:     aws.String(me.bucketName), // Required
		CopySource: aws.String(copySrc),
		Key:        aws.String(tgtName),  // Required
		PartNumber: aws.Int64(pnum),      // Required
		UploadId:   aws.String(uploadId), // Required
	}
	log.Println(copySrc)
	if len(byteRange) > 0 {
		params.CopySourceRange = aws.String(byteRange)
	}

	rsp, err := me.svc.UploadPartCopy(params)
	if err != nil {
		log.Println(err.Error())
		return nil, fscommon.EIO
	}

	log.Println(rsp.CopyPartResult.ETag)

	return &s3.CompletedPart{PartNumber: &pnum, ETag: rsp.CopyPartResult.ETag}, 0
}

func (me *S3FileIO) copyBigPart(tgt string, src string, srcLen int64, uploadId string, pnum int64) ([]*s3.CompletedPart, int64) {
	plist := make([]*s3.CompletedPart, (srcLen/S3_MAX_BLOCK_SIZE)+10)
	remainLen := srcLen
	var start, end int64 = 0, 0
	for remainLen > 0 {
		if remainLen >= 2*S3_MAX_BLOCK_SIZE {
			start = end
			end = start + S3_MAX_BLOCK_SIZE
			remainLen -= S3_MAX_BLOCK_SIZE
		} else if remainLen >= S3_MAX_BLOCK_SIZE && remainLen < 2*S3_MAX_BLOCK_SIZE {
			start = end
			end = start + (remainLen / 2)
			remainLen -= (remainLen / 2)
		} else {
			start = end
			end = start + remainLen
			remainLen = 0
		}
		byteRange := fmt.Sprintf("bytes=%d-%d", start, end)
		cp, ok := me.copyPart(tgt, src, byteRange, uploadId, pnum)
		if ok < 0 {
			me.cleanMultipartUpload(tgt, uploadId)
			return nil, int64(ok)
		}
		plist = append(plist, cp)
		pnum++
	}

	return plist, pnum
}

func (me *S3FileIO) uploadPart(tgtName string, data []byte, uploadId string, pnum int64) (*s3.CompletedPart, int) {
	params := &s3.UploadPartInput{
		Bucket:        aws.String(me.bucketName), // Required
		Key:           aws.String(tgtName),       // Required
		PartNumber:    aws.Int64(pnum),           // Required
		UploadId:      aws.String(uploadId),      // Required
		Body:          bytes.NewReader(data),
		ContentLength: aws.Int64(int64(len(data))),
	}

	rsp, err := me.svc.UploadPart(params)
	if err != nil {
		log.Println(err.Error())
		return nil, fscommon.EIO
	}
	return &s3.CompletedPart{PartNumber: &pnum, ETag: rsp.ETag}, 0
}

func (me *S3FileIO) copyFile(tgt string, src string) int {
	params := &s3.CopyObjectInput{
		Bucket:     aws.String(me.bucketName), // Required
		CopySource: aws.String(src),           // Required
		Key:        aws.String(tgt),           // Required
	}

	_, err := me.svc.CopyObject(params)
	if err != nil {
		log.Println(err.Error())
		return fscommon.EIO
	}

	return 0
}

func (me *S3FileIO) copyFileByRange(tgt string, src string, byteRange string) int {
	uploadId, ok := me.startUpload(tgt)
	if ok != 0 {
		return ok
	}

	cp, ok := me.copyPart(tgt, src, byteRange, uploadId, 1)
	if ok != 0 {
		return ok
	}

	plist := make([]*s3.CompletedPart, 1)
	plist = append(plist, cp)
	ok = me.completeUpload(tgt, uploadId, plist)
	return ok
}

func (me *S3FileIO) PutBuffer(name string, data []byte) int {
	/* 	md := map[string]*string{
	        "x-csm-file-slice-size": 	aws.String(string(FILE_SLICE_SIZE)),
			"x-csm-slice-file"	   :    aws.String("1"),
	    } */
	params := &s3.PutObjectInput{
		Bucket: aws.String(me.bucketName), // Required
		Key:    aws.String(name),          // Required
		Body:   bytes.NewReader(data),
		//Metadata:			md,
	}

	_, err := me.svc.PutObject(params)
	if err != nil {
		log.Println(err.Error())
		return fscommon.EIO
	}

	return len(data)
}

func (me *S3FileIO) ZeroFile(name string) int {
	return me.PutBuffer(name, make([]byte, 0))
}

func (me *S3FileIO) GetBuffer(name string, dest []byte, offset int64) int {

	byteRange := fmt.Sprintf("bytes=%d-%d", offset, offset+int64(len(dest))-1)
	log.Println("Read file, range = ", byteRange)

	params := &s3.GetObjectInput{
		Bucket: aws.String(me.bucketName),
		Key:    aws.String(name),
		Range:  aws.String(byteRange),
	}
	rsp, err := me.svc.GetObject(params)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			//by now, I can only see that AWS S3 return this code when file size is zero
			if awsErr.Code() == "InvalidRange" {
				return 0
			} else if awsErr.Code() == "NoSuchKey" {
				return fscommon.ENOENT
			}
		}
		log.Println(err.Error())
		return fscommon.EIO
	}

	//n := rsp.ContentLength

	n, err := rsp.Body.Read(dest)
	if err != nil && err != io.EOF {
		log.Println(err.Error())
		return fscommon.EIO
	}

	return n
}

func (me *S3FileIO) GetAttr(path string) (os.FileInfo, int) {
	return me.fs._getAttrFromRemote(path, fscommon.S_IFREG)
	//sliceSize := rsp.Metadata["x-csm-file-slice-size"]
	//sliceCount := rsp.Metadata["x-csm-file-slice-count"]
	//isSliced  := rsp.Metadata["x-csm-slice-file"]
	//fileRealSize := rsp.Metadata["x-csm-file-size"]
}

func (me *S3FileIO) WaitFileReady(path string) int {
	ok := fscommon.ENOENT
	n := 10
	for n > 0 {
		di, ok := me.GetAttr(path)
		if ok < 0 {
			return ok
		}
		if di.Size() >= 0 {
			ok = 0
			break
		}
		time.Sleep(2 * time.Second)
	}

	return ok
}

func (me *S3FileIO) Rename(src string, tgt string) int {
	uploadId, ok := me.startUpload(tgt)
	if ok < 0 {
		return ok
	}
	var pnum int64 = 1
	var start int = 0
	partList := make([]*s3.CompletedPart, 2)
	for true {
		byteRange := fmt.Sprintf("bytes=%d-%d", start, start+S3_MAX_PART_SIZE-1)
		part, ok := me.copyPart(tgt, src, byteRange, uploadId, pnum)
		if ok < 0 {
			break
		}
		partList = append(partList, part)
		pnum++
		start += S3_MAX_PART_SIZE
	}

	ok = me.completeUpload(tgt, uploadId, partList)
	if ok < 0 { //cancel the upload
		//failed to upload? then cancel the upload request
		params := &s3.AbortMultipartUploadInput{
			Bucket:   aws.String(me.bucketName),
			Key:      aws.String(tgt),
			UploadId: aws.String(uploadId),
		}
		_, err := me.svc.AbortMultipartUpload(params)
		if err != nil {
			log.Println(err.Error())
		}

		return ok
	}

	me.fs.Unlink(src)

	return ok
}

func (me *S3FileIO) Unlink(name string) int {
	return me.fs.Unlink(name)
}

func (me *S3FileIO) ListFile(path string) ([]os.FileInfo, int) {

	prefix := path
	if len(prefix) != 0 && prefix != "/" {
		prefix = prefix + "/"
	}

	params := &s3.ListObjectsV2Input{
		Bucket: aws.String(me.bucketName), // Required
		//ContinuationToken: aws.String("Token"),
		Delimiter: aws.String("/"),
		//EncodingType:      aws.String("EncodingType"),
		//FetchOwner:        aws.Bool(true),
		//MaxKeys:           aws.Int64(1),
		Prefix: aws.String(prefix),
		//StartAfter:        aws.String("StartAfter"),
	}
	rsp, err := me.svc.ListObjectsV2(params)

	if err != nil { // resp is not filled
		log.Println(err)
		return nil, -1
	}

	diCount := len(rsp.Contents)

	dis := make([]os.FileInfo, diCount)

	//collect files
	j := 0
	for i := 0; i < len(rsp.Contents); i++ {
		key := *(rsp.Contents[i].Key)
		if key == prefix {
			continue
		}

		di := &fscommon.DirItem{
			DiName:  key, //need remove common prefix
			DiSize:  *(rsp.Contents[i].Size),
			DiMtime: *rsp.Contents[i].LastModified,
			//rsp.Contents[i].ETag
			DiType: fscommon.S_IFREG,
		}
		dis[j] = di

		j++
	}

	return dis, diCount
}

func (me *S3FileIO) cleanMultipartUpload(path string, uploadId string) int {
	return 0
}
