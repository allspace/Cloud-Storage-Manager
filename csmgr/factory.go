package main

import(	
	"os"
	"log"
	
	"brightlib.com/common"
	"brightlib.com/drivers/s3impl"
	"brightlib.com/drivers/aliyun"
)
 

func NewFileSystem(name string)(fscommon.FileSystemImpl, int) {
	var client fscommon.ClientImpl
	switch name {
		case "S3":
			client = s3impl.NewClient()
			break
		case "Aliyun":
		    client = aliyunimpl.NewClient()
			break
	}
	
	
	endPoint := os.Getenv("CS_ENDPOINT")
	keyId    := os.Getenv("CS_KEY_ID")
	key      := os.Getenv("CS_KEY_DATA")
	bucket   := os.Getenv("CS_BUCKET")
	region   := os.Getenv("CS_REGION")
	
	log.Println(endPoint)
	
	if len(endPoint)>0 {
		client.Set("EndPoint", endPoint)
	}
	client.Connect(region, keyId, key)
	fs,_ := client.Mount(bucket)
	
	return fs, 0
}

