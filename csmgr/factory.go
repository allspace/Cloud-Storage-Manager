package main

import (
	"log"
	"strings"
	//"os"

	"brightlib.com/common"
	cfg "brightlib.com/config"
	"brightlib.com/drivers/aliyun"
	"brightlib.com/drivers/s3impl"
)

func NewFileSystem(name string) (fscommon.FileSystemImpl, int) {
	var client fscommon.ClientImpl
	switch strings.ToLower(name) {
	case "s3":
		client = s3impl.NewClient()
		break
	case "aliyun":
		client = aliyunimpl.NewClient()
		break
	default:
		log.Printf("Unknown vendor type: %s.", name)
		return nil, -1
	}

	endPoint, _ := cfg.Default.GetString("ENDPOINT")
	keyId, _ := cfg.Default.GetString("KEY_ID")
	key, _ := cfg.Default.GetString("KEY_DATA")
	bucket, _ := cfg.Default.GetString("BUCKET")
	region, _ := cfg.Default.GetString("REGION")

	log.Println(endPoint)

	if len(endPoint) > 0 {
		client.Set("EndPoint", endPoint)
	}
	client.Connect(region, keyId, key)
	fs, _ := client.Mount(bucket)

	return fs, 0
}
