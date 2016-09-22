package aliyunimpl

import (
    //"os"
    "log"
    "github.com/aliyun/aliyun-oss-go-sdk/oss"
	
	"brightlib.com/common"
)

type AliyunClientImpl struct {
    client *oss.Client
	
	cfg 		map[string]string
}

func NewClient() (*AliyunClientImpl) {
	return &AliyunClientImpl{cfg: make(map[string]string, 100)}
}

func (me *AliyunClientImpl) Set(key string, value string) {
	me.cfg[key] = value
}

func (me *AliyunClientImpl) Connect(region string, keyId string, keyData string)(int) {
    var err error
	
	endPoint := region + ".aliyuncs.com"
	
    me.client, err = oss.New(endPoint, keyId, keyData)
    if err != nil {
        log.Println(err)
        return fscommon.EIO //EIO
    }
    return 0
}

func (me *AliyunClientImpl) Disconnect()(int) {
	return 0
}

func (me *AliyunClientImpl) Mount(bucketName string)(fscommon.FileSystemImpl, int) {
    bucket,err := me.client.Bucket(bucketName)
    if err != nil {
        log.Println(err)
        return nil,fscommon.EIO //EIO
    }
	log.Println("Connected to bucket ", bucketName)
	vol := &AliyunFSImpl{
			client		: me.client,
			bucket      : bucket,
			bucketName	: bucketName,
			dirCache	: fscommon.NewDirCache(),
			fileMgr		: fscommon.NewFileInstanceMgr(),
		}
	return vol, 0
}

func (me *AliyunClientImpl) UnMount(bucketName string)(int) {
	return 0
}
