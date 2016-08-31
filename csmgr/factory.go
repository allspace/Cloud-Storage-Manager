package main

import(	
	"os"
	
	"brightlib.com/common"
	"brightlib.com/drivers/s3impl"
)
 

func NewFileSystem(name string)(fscommon.FileSystemImpl, int) {

	client := s3impl.NewClient()
	
	endPoint := os.Getenv("AWS_ENDPOINT")
	keyId    := os.Getenv("AWS_KEY_ID")
	key      := os.Getenv("AWS_ACCESS_KEY")
	bucket   := os.Getenv("AWS_S3_BUCKET")
	region   := os.Getenv("AWS_REGION")
	//fmt.Println(endPoint)
	
	if len(endPoint)>0 {
		client.Set("EndPoint", endPoint)
	}
	client.Connect(region, keyId, key)
	fs,_ := client.Mount(bucket)
	
	return fs, 0
}

