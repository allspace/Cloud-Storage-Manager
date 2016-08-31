package s3impl

import (
	//"syscall"
	"fmt"
	"io"
	//"strings"
	
	"github.com/aws/aws-sdk-go/aws"
	//"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type S3FileImpl struct {
	awsConfig *aws.Config
	sess      *session.Session
	svc        *s3.S3
	
	bucketName  string
	fileName    string
	openFlags	uint32
}


func (me *S3FileImpl) Read(dest []byte, off int64)(int) {

	byteRange := fmt.Sprintf("bytes=%d-%d", off, off + int64(len(dest)) -1)
	fmt.Println("Read file, range = ", byteRange)
	
	params := &s3.GetObjectInput{
		Bucket		: aws.String(me.bucketName),
		Key 		: aws.String(me.fileName),
		Range		: aws.String(byteRange),
	}
	rsp, err := me.svc.GetObject(params)
	if err != nil {
		fmt.Println(err.Error())
		return -1
	}
	
	//n := rsp.ContentLength
	
	n, err := rsp.Body.Read(dest)
	if err != nil && err !=io.EOF {
		fmt.Println(err.Error())
		return -1
	}
	
	return n
}
