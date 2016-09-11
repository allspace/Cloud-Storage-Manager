package s3impl

import (
	//"syscall"
	"fmt"
	"io"
	"bytes"
	"log"
	"time"
	
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	//"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"brightlib.com/common"
)

const (
	S3_MAX_PART_SIZE = 5 * 1024 * 1024 * 1024
)

type S3FileIO struct {
	svc        *s3.S3
	fs		   *S3FileSystemImpl
	bucketName  string
}


func (me *S3FileIO) startUpload(name string) (string, int) {

	params := &s3.CreateMultipartUploadInput{
		Bucket:             aws.String(me.bucketName), // Required
		Key:                aws.String(name),  // Required
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
		Bucket:   aws.String(me.bucketName),        // Required
		Key:      aws.String(name),          // Required
		UploadId: aws.String(uploadId), 			// Required
		MultipartUpload: &s3.CompletedMultipartUpload{
								Parts: 	plist,
		},
	}
	_, err := me.svc.CompleteMultipartUpload(params)
	if err != nil {
		log.Println(err.Error())
		return fscommon.EIO
	}
	
	return 0
}

func (me *S3FileIO) copyPart(tgtName string, srcName string, byteRange string, uploadId string, pnum int64) (*s3.CompletedPart,int) {
	params := &s3.UploadPartCopyInput{
		Bucket:               aws.String(me.bucketName),        // Required
		CopySource:			  aws.String(srcName),
		Key:                  aws.String(tgtName),         		// Required
		PartNumber:           aws.Int64(pnum),                    // Required
		UploadId:             aws.String(uploadId), 			// Required
	}
	log.Println(byteRange)
	if len(byteRange)>0 {
		params.CopySourceRange = aws.String(byteRange)
	}
	
	rsp, err := me.svc.UploadPartCopy(params)
	if err != nil {
		log.Println(err.Error())
		return nil,fscommon.EIO
	}
	return &s3.CompletedPart{PartNumber : &pnum, ETag : rsp.CopyPartResult.ETag}, 0
}

func (me *S3FileIO) uploadPart(tgtName string, data []byte, uploadId string, pnum int64) (*s3.CompletedPart,int) {
	params := &s3.UploadPartInput{
		Bucket:               aws.String(me.bucketName),    // Required
		Key:                  aws.String(tgtName),         	// Required
		PartNumber:           aws.Int64(pnum),              // Required
		UploadId:             aws.String(uploadId), 		// Required
		Body:                 bytes.NewReader(data),
		ContentLength:        aws.Int64(int64(len(data))),
	}
	
	rsp, err := me.svc.UploadPart(params)
	if err != nil {
		log.Println(err.Error())
		return nil,fscommon.EIO
	}
	return &s3.CompletedPart{PartNumber : &pnum, ETag : rsp.ETag}, 0
}


func (me *S3FileIO) putBlock(name string, data []byte) int {
	params := &s3.PutObjectInput{
		Bucket:             aws.String(me.bucketName),  // Required
		Key:                aws.String(name),  			// Required
		Body:               bytes.NewReader(data),
	}
	
	_, err := me.svc.PutObject(params)
	if err != nil {
		log.Println(err.Error())
		return fscommon.EIO
	}
	
	return len(data)
}

func (me *S3FileIO) getBuffer(name string, dest []byte, offset int64) int {

	byteRange := fmt.Sprintf("bytes=%d-%d", offset, offset + int64(len(dest)) -1)
	log.Println("Read file, range = ", byteRange)
	
	params := &s3.GetObjectInput{
		Bucket		: aws.String(me.bucketName),
		Key 		: aws.String(name),
		Range		: aws.String(byteRange),
	}
	rsp, err := me.svc.GetObject(params)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			//by now, I can only see that AWS S3 return this code when file size is zero
			if awsErr.Code() == "InvalidRange" {
				return 0
			}
		}
		log.Println(err.Error())
		return fscommon.EIO
	}
	
	//n := rsp.ContentLength
	
	n, err := rsp.Body.Read(dest)
	if err != nil && err !=io.EOF {
		log.Println(err.Error())
		return fscommon.EIO
	}
	
	return n
}

func (me *S3FileIO) GetRemoteFileSize(path string)(int64) {
	key := path
	params := &s3.HeadObjectInput{
		Bucket:         aws.String(me.bucketName), // Required
		Key:            aws.String(key),  // Required
	}
	
	rsp, err := me.svc.HeadObject(params)
	if err != nil {
		log.Println(err.Error())
		return fscommon.ENOENT		//fail to get attributes
	}

	return int64(*rsp.ContentLength)
}

func (me *S3FileIO) WaitFileReady(path string)(int) {
	ok := fscommon.ENOENT
	n := 10
	for n > 0 {
		size := me.GetRemoteFileSize(path)
		if size>=0 {
			ok = 0
			break
		}
		time.Sleep(2 * time.Second)
	}
	
	return ok
}

func (me *S3FileIO) Rename(src string, tgt string)(int) {
	uploadId,ok := me.startUpload(tgt)
	if ok < 0 {
		return ok
	}
	var pnum int64 = 1
	var start int = 0
	partList := make([]*s3.CompletedPart, 2)
	for true {
		byteRange := fmt.Sprintf("bytes=%d-%d", start, start + S3_MAX_PART_SIZE -1)
		part,ok := me.copyPart(tgt, src, byteRange, uploadId, pnum)
		if ok < 0 {
			break
		}
		partList = append(partList, part)
		pnum++
		start += S3_MAX_PART_SIZE
	}
	
	ok = me.completeUpload(tgt, uploadId, partList)
	if ok < 0 {		//cancel the upload
		//failed to upload? then cancel the upload request
		params := &s3.AbortMultipartUploadInput {
			Bucket	 : aws.String(me.bucketName),
			Key 	 : aws.String(tgt),
			UploadId : aws.String(uploadId),
		}
		_,err := me.svc.AbortMultipartUpload(params)
		if err != nil {
			log.Println(err.Error())
		}
		
		return ok
	}
	
	me.fs.Unlink(src)
	
	return ok
}

func (me *S3FileIO) Unlink(name string)(int) {
	return me.fs.Unlink(name)
}


