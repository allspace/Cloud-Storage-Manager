package s3impl

import (
	//"syscall"
	"fmt"
	"io"
	"bytes"
	"log"
	
	"github.com/aws/aws-sdk-go/aws"
	//"github.com/aws/aws-sdk-go/aws/awserr"
	//"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)



type S3FileIO struct {
	svc        *s3.S3
	bucketName  string
}


func (me *S3FileIO) startUpload(name string) (string, int) {

	params := &s3.CreateMultipartUploadInput{
		Bucket:             aws.String(me.bucketName), // Required
		Key:                aws.String(name),  // Required
	}
	rsp, err := me.svc.CreateMultipartUpload(params)
	if err != nil {
		return "", -1
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
		return -1
	}
	
	return 0
}

func (me *S3FileIO) copyPart(tgtName string, srcName string, uploadId string, pnum int64) (*s3.CompletedPart,int) {
	params := &s3.UploadPartCopyInput{
		Bucket:               aws.String(me.bucketName),        // Required
		CopySource:			  aws.String(srcName),
		Key:                  aws.String(tgtName),         // Required
		PartNumber:           aws.Int64(pnum),                    // Required
		UploadId:             aws.String(uploadId), 			// Required
	}
	rsp, err := me.svc.UploadPartCopy(params)
	if err != nil {
		return nil,-1
	}
	return &s3.CompletedPart{PartNumber : &pnum, ETag : rsp.CopyPartResult.ETag}, 0
}

func (me *S3FileIO) putBlock(name string, data []byte) int {
	params := &s3.PutObjectInput{
		Bucket:             aws.String(me.bucketName),  // Required
		Key:                aws.String(name),  			// Required
		Body:               bytes.NewReader(data),
	}
	
	_, err := me.svc.PutObject(params)
	if err != nil {
		return -1
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
		log.Println(err.Error())
		return -1
	}
	
	//n := rsp.ContentLength
	
	n, err := rsp.Body.Read(dest)
	if err != nil && err !=io.EOF {
		log.Println(err.Error())
		return -1
	}
	
	return n
}


