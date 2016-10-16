package aliyunimpl

import (
	"io"
	"os"
	//"os"
	//"time"
	"bytes"
	"log"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"

	"github.com/allspace/csmgr/common"
)

type AliyunIO struct {
	bucket     *oss.Bucket
	bucketName string

	fs *AliyunFSImpl
}

///////////////////////////////////////////////////////////////////////////////
//Exported functions
///////////////////////////////////////////////////////////////////////////////

func (me *AliyunIO) PutBuffer(name string, data []byte) int {
	if name[0] == '/' {
		name = name[1:]
	}

	err := me.bucket.PutObject(name, bytes.NewReader(data))
	if err != nil {
		log.Println(err)
		return fscommon.EIO
	}
	return 0
}

func (me *AliyunIO) GetBuffer(name string, dest []byte, offset int64) int {
	if name[0] == '/' {
		name = name[1:]
	}
	//log.Printf("GetBuffer: offset %d length %d", offset, len(dest))
	body, err := me.bucket.GetObject(name, oss.Range(offset, offset+int64(len(dest))))
	if err != nil {
		log.Println(name + " : " + err.Error())
		if se, ok := err.(oss.ServiceError); ok {
			if se.StatusCode == 404 {
				return fscommon.ENOENT
			}
		}
		return fscommon.EIO
	}

	n, _ := io.ReadFull(body, dest)
	body.Close()
	//log.Printf("n=%d", n)
	return n
}

func (me *AliyunIO) AppendBuffer(name string, dest []byte, offset int64) int64 {
	nextPos, err := me.bucket.AppendObject(name, bytes.NewReader(dest), offset)
	if err != nil {
		log.Println(err)
		return fscommon.EIO
	}
	return nextPos
}

func (me *AliyunIO) ListFile(path string) ([]os.FileInfo, int) {
	prefix := path
	if len(prefix) != 0 && prefix[0] == '/' {
		prefix = prefix[1:]
	}

	lsRes, err := me.bucket.ListObjects(oss.Prefix(prefix), oss.Delimiter("/"))
	if err != nil {
		log.Println(err)
		return nil, fscommon.EIO
	}

	dis := make([]os.FileInfo, len(lsRes.Objects))

	//collect files
	j := 0
	for i := 0; i < len(lsRes.Objects); i++ {
		key := lsRes.Objects[i].Key
		if key == prefix {
			continue
		}

		dis[j] = &fscommon.DirItem{
			DiName:  fscommon.GetLastPathComp(key), //need remove common prefix
			DiSize:  lsRes.Objects[i].Size,
			DiMtime: lsRes.Objects[i].LastModified,
			//lsRes.Objects[i].ETag
			DiType: fscommon.S_IFREG,
		}
		j++
	}
	return dis, 0
}

func (me *AliyunIO) Unlink(path string) int {
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

func (me *AliyunIO) ZeroFile(name string) int {
	return 0
}

func (me *AliyunIO) GetAttr(path string) (os.FileInfo, int) {
	return me.fs.getAttrFromRemote(path, fscommon.S_IFREG)
}
