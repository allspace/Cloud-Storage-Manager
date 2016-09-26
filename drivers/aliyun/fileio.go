package aliyunimpl

import (
    //"os"
	//"time"
    "log"
	"bytes"
    "github.com/aliyun/aliyun-oss-go-sdk/oss"
	
	"brightlib.com/common"
)

type AliyunIO struct {
    bucket *oss.Bucket
	fs     *AliyunFSImpl
	bucketName string
}

func (me *AliyunIO) PutBuffer(name string, data []byte)(int) {
    err := me.bucket.PutObject(name, bytes.NewReader(data))
    if err != nil {
        log.Println(err)
		return fscommon.EIO
    }
	return 0
}

func (me *AliyunIO) GetBuffer(name string, dest []byte, offset int64)(int) {
    if name[0] == '/' {
	    name = name[1:]
	}
    body, err := me.bucket.GetObject(name, oss.Range(offset, offset+int64(len(dest))))
    if err != nil {
        log.Println(name + " : " + err.Error())
		if se,ok := err.(oss.ServiceError); ok {
		    if se.StatusCode == 404 {
			    return fscommon.ENOENT
			}
		}
		return fscommon.EIO
    }
	body.Read(dest)
    body.Close()
	return len(dest)
}

func (me *AliyunIO) AppendBuffer(name string, dest []byte, offset int64)(int64) {
	nextPos, err := me.bucket.AppendObject(name, bytes.NewReader(dest), offset)
	if err != nil {
	    log.Println(err)
		return fscommon.EIO
	}
	return nextPos
}

func (me *AliyunIO) ListFile(path string) ([]fscommon.DirItem , int) { 
	prefix := path
	if len(prefix) != 0 && prefix[0] == '/' {
		prefix = prefix[1:]
	}

	lsRes, err := me.bucket.ListObjects(oss.Prefix(prefix), oss.Delimiter("/"))
	if err != nil {
		log.Println(err)
		return nil, fscommon.EIO
	}
	
	dis := make([]fscommon.DirItem,  len(lsRes.Objects))
	
	//collect files
	j := 0
	for i := 0; i<len(lsRes.Objects); i++ {
		key := lsRes.Objects[i].Key
		if key == prefix {
			continue
		}
		
		dis[j] = fscommon.DirItem{
					Name : fscommon.GetLastPathComp(key),		//need remove common prefix
					Size : uint64(lsRes.Objects[i].Size),
					Mtime: lsRes.Objects[i].LastModified,
					//lsRes.Objects[i].ETag 
					Type : fscommon.S_IFREG,
				}
		j++
	}
	return dis, 0
}

func (me *AliyunIO) Unlink(path string)(int) {
	//check if it's a file. we only deal with file here
	if path[len(path)-1] == '/' {
		return fscommon.EINVAL
	}
	
	err := me.bucket.DeleteObject(path)
	if err != nil {
		log.Println(err)
		return fscommon.EIO
	}
	return 0
}

func (me *AliyunIO) GetRemoteFileSize(path string)(int64) {
    return 0
}

func (me *AliyunIO) ZeroFile(name string)(int) {
    return 0
}