package aliyunimpl

import (
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"

	"github.com/allspace/csmgr/common"
)

type AliyunFSImpl struct {
	fscommon.FSImplBase
	client *oss.Client
	bucket *oss.Bucket
}

///////////////////////////////////////////////////////////////////////////////
//Internal functions
///////////////////////////////////////////////////////////////////////////////

func (me *AliyunFSImpl) addDirCache(key string, di *fscommon.DirItem) {
	if key[len(key)-1] == '/' {
		key = key[:len(key)-1]
	}
	log.Println("Add dir cache: ", key)
	me.DirCache.Add(key, di, fscommon.CACHE_LIFE_SHORT)
}

//get attributes for path/file
//it can also be used to check if path/file exists
func (me *AliyunFSImpl) getAttrFromRemote(path string, iType int) (os.FileInfo, int) {
	key := path
	if len(path) != 0 && path[0] == '/' {
		key = path[1:]
	}

	if iType == fscommon.S_IFDIR {
		key = key + "/"
	}
	log.Printf("Get object detail for %s", key)
	meta, err := me.bucket.GetObjectDetailedMeta(key)
	if err != nil {
		// Print the error, cast err to awserr.Error to get the Code and
		// Message from an error.
		if reqerr, ok := err.(oss.ServiceError); ok {
			log.Printf("reqerr.StatusCode=%d", reqerr.StatusCode)
			if reqerr.StatusCode == 404 {
				if iType == fscommon.S_IFUNKOWN {
					return me.getAttrFromRemote(key, fscommon.S_IFDIR)
				} else {
					return nil, fscommon.ENOENT
				}
			}
		}
		log.Println(path, " : ", err.Error())
		return nil, fscommon.ENOENT //fail to get attributes
	}
	if iType != fscommon.S_IFDIR {
		iType = fscommon.S_IFREG
	}
	var size int64
	size, err = strconv.ParseInt(meta["Content-Length"][0], 10, 64)
	if err != nil {
		size = 0
	}
	const longForm = "Jan 2, 2006 at 3:04pm (MST)"

	//log.Println(path)
	//log.Println(meta["LastModified"])

	var mtime time.Time
	if len(meta["LastModified"]) > 0 {
		mtime, _ = time.Parse(longForm, meta["LastModified"][0])
	}
	return &fscommon.DirItem{
		DiName:  fscommon.GetLastPathComp(path),
		DiType:  iType,
		DiSize:  size,
		DiMtime: mtime,
	}, 0
}

///////////////////////////////////////////////////////////////////////////////
//Exported functions
///////////////////////////////////////////////////////////////////////////////

func (me *AliyunFSImpl) GetAttr(path string) (os.FileInfo, int) {
	log.Printf("GetAttr is called for %s", path)
	if len(path) > 1 && path[0] == '/' {
		path = path[1:]
	}

	di, ok := me.FSImplBase.GetAttr(path)
	if di != nil || ok != 0 {
		return di, ok
	}

	//get attributes from remote
	di, ok = me.getAttrFromRemote(path, fscommon.S_IFUNKOWN)
	if ok == fscommon.ENOENT {
		me.NotExistCache.Add(path, &fscommon.DirItem{}, fscommon.CACHE_LIFE_SHORT)
	}
	return di, ok
}

func (me *AliyunFSImpl) OpenDir(path string) int {
	return 0
}

func (me *AliyunFSImpl) ReadDir(path string) ([]os.FileInfo, int) {

	log.Println("AliyunFSImpl::ReadDir = ", path)

	prefix := path
	if len(prefix) != 0 && prefix[0] == '/' {
		prefix = prefix[1:]
	}
	if len(prefix) > 0 && prefix[len(prefix)-1] != '/' {
		prefix = prefix + "/"
	}

	lsRes, err := me.bucket.ListObjects(oss.Prefix(prefix), oss.Delimiter("/"))
	if err != nil {
		log.Println(err)
		return nil, fscommon.EIO
	}

	diCount := len(lsRes.CommonPrefixes) + len(lsRes.Objects)

	dis := make([]os.FileInfo, diCount)

	//collect directories
	for i := 0; i < len(lsRes.CommonPrefixes); i++ {
		key := lsRes.CommonPrefixes[i]
		if key == prefix {
			continue
		}
		//hide cache/tmp dir or slice group
		if key[0] == '$' && key[len(key)-1] == '$' {
			continue
		}

		dis[i] = &fscommon.DirItem{
			DiName: fscommon.GetLastPathComp(key), //need remove slash suffix and common prefix (for subdir)
			DiType: fscommon.S_IFDIR,
			DiSize: 0,
		}
		//add to cache
		me.addDirCache(key, dis[i].(*fscommon.DirItem))
	}

	//collect files
	j := len(lsRes.CommonPrefixes)
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

		//add to cache
		me.addDirCache(key, dis[j].(*fscommon.DirItem))
		j++
	}

	log.Println("Directories and files: ", diCount)
	return []os.FileInfo(dis), diCount
}

func (me *AliyunFSImpl) NewFileImpl(path string) (fscommon.FileImpl, int) {
	fio := &AliyunIO{
		bucket:     me.bucket,
		fs:         me,
		bucketName: me.BucketName,
	}

	rc := NewAliyunFile(fio)
	return rc, 0
}

func (me *AliyunFSImpl) Open(path string, flags uint32) (*fscommon.FileObject, int) {
	if len(path) != 0 && path[0] == '/' {
		path = path[1:]
	}
	//look in file instance manager first
	//if successful, this will increase instance reference count
	log.Println("Try to find existing object for file", path)

	fo, ok := me.FileMgr.GetInstance(path)
	if ok == 0 {
		log.Println("Found existing instance for file ", path)
		return fo, 0
	}
	log.Println("Verify existing for file", path)
	//var fileNotExist bool = false

	//verify if the file exists, and if user has permission to open the file in selected mode
	_, ok = me.getAttrFromRemote(path, fscommon.S_IFREG)
	switch ok {
	case fscommon.EIO:
		return nil, ok
	case fscommon.ENOENT:
		if (flags & fscommon.O_CREAT) == 0 {
			log.Printf("File %s does not exist, but open it without O_CREAT flag\n", path)
			return nil, ok
		}
		//fileNotExist = true
		break
	}

	log.Println("Trying to allocate a file instance for file ", path)
	fo, ok = me.FileMgr.Allocate(me, path)
	if ok != 0 {
		return nil, ok
	}
	/* 	if ok==0 && fileNotExist==true {
		me.dirCache.Add(path,&fscommon.DirItem{
			Name : path,
			Size : 0,
			Type : fscommon.S_IFREG,
			Mtime: time.Now(),
		}, fscommon.CACHE_LIFE_LONG)
	} */

	//call this function here to avoid running it in big lock context
	ok = fo.Open(path, flags)
	if ok == 0 {
		return fo, ok
	} else {
		return nil, ok
	}
}

func (me *AliyunFSImpl) Chmod(name string, mode uint32) int {
	return 0
}

func (me *AliyunFSImpl) Utimens(name string, Mtime *time.Time) int {
	return 0
}

func (me *AliyunFSImpl) Mkdir(path string, mode uint32) int {
	var key string
	//make sure we are going to create a directory
	if path[len(path)-1] != '/' {
		key = path + "/"
	} else {
		key = path
	}

	//Aliyun does not allow leading slash or back slash
	if key[0] == '/' {
		key = key[1:]
	}

	//not allow create root directory
	if len(key) == 0 {
		return fscommon.EINVAL
	}

	err := me.bucket.PutObject(key, strings.NewReader(""))
	if err != nil {
		log.Println(path, " : ", err)
		return fscommon.EIO
	}
	return 0
}

func (me *AliyunFSImpl) Unlink(path string) int {
	//check if it's a file. we only deal with file here
	if path[len(path)-1] == '/' {
		return fscommon.EINVAL
	}

	//if the file is being open, return status busy
	if me.FileMgr.Exist(path) {
		return fscommon.EBUSY
	}

	//no leading slash for aliyun
	if len(path) > 0 && path[0] == '/' {
		path = path[1:]
	}

	err := me.bucket.DeleteObject(path)
	if err != nil {
		log.Println(err)
		return fscommon.EIO
	}
	return 0
}
