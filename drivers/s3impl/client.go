package s3impl

import (
	"log"
	
	"github.com/aws/aws-sdk-go/aws"
	//"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"brightlib.com/common"
)

type S3ClientImpl struct {
	cfg 		map[string]string
	awsConfig 	*aws.Config
	sess      	*session.Session
	s3        	*s3.S3
}

func NewClient() (*S3ClientImpl) {
	return &S3ClientImpl{cfg: make(map[string]string, 100)}
}

func (me *S3ClientImpl) Set(key string, value string) {
	me.cfg[key] = value
}

func (me *S3ClientImpl) Connect(region string, keyId string, skey string)(int) {
	config := &aws.Config{
				Region		: aws.String(region),
				Credentials	: credentials.NewStaticCredentials(keyId, skey, ""),
			}
			
	//set end point if user specified it
	if endPoint,ok := me.cfg["EndPoint"]; ok {
		config.WithEndpoint(endPoint)
	}
	log.Println(region)
	
	//config.WithLogLevel(aws.LogDebug)
	
	me.sess = session.New(config)
	me.s3 = s3.New(me.sess)
	
	return 0
}

func (me *S3ClientImpl) Disconnect()(int) {
	return 0
}

func (me *S3ClientImpl) Mount(bucketName string)(*S3FileSystemImpl, int) {
	vol := &S3FileSystemImpl{
			svc			: me.s3,
			bucketName	: bucketName,
			dirCache	: fscommon.NewDirCache(),
			fileMgr		: fscommon.NewFileInstanceMgr(),
		}
	return vol, 0
}

func (me *S3ClientImpl) UnMount(bucketName string)(int) {
	return 0
}

