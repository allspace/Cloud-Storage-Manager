package aliyunimpl

import (
	//"os"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"

	"brightlib.com/common"
)

type AliyunFSImpl struct {
	client *oss.Client
	bucket *oss.Bucket

	bucketName    string
	dirCache      *fscommon.DirCache
	notExistCache *fscommon.DirCache //avoid flag files being checked too frequently
	fileMgr       *fscommon.FileInstanceMgr
}

///////////////////////////////////////////////////////////////////////////////
//Internal functions
///////////////////////////////////////////////////////////////////////////////

func (me *AliyunFSImpl) addDirCache(key string, di fscommon.DirItem) {
	if key[len(key)-1] == '/' {
		key = key[:len(key)-1]
	}
	me.dirCache.Add(key, &di, fscommon.CACHE_LIFE_SHORT)
}

//get attributes for path/file
//it can also be used to check if path/file exists
func (me *AliyunFSImpl) _getAttrFromRemote(path string, iType int) (*fscommon.DirItem, int) {
	key := path
	if iType == fscommon.S_IFDIR {
		key = key + "/"
	}
	meta, err := me.bucket.GetObjectDetailedMeta(key)
	if err != nil {
		// Print the error, cast err to awserr.Error to get the Code and
		// Message from an error.
		if reqerr, ok := err.(oss.ServiceError); ok {
			if reqerr.StatusCode == 404 {
				if iType == fscommon.S_IFUNKOWN {
					return me._getAttrFromRemote(key, fscommon.S_IFDIR)
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

	log.Println(path)
	log.Println(meta["LastModified"])

	var mtime time.Time
	if len(meta["LastModified"]) > 0 {
		mtime, _ = time.Parse(longForm, meta["LastModified"][0])
	}
	return &fscommon.DirItem{
		Name:  fscommon.GetLastPathComp(path),
		Type:  iType,
		Size:  uint64(size),
		Mtime: mtime,
	}, 0
}

///////////////////////////////////////////////////////////////////////////////
//Exported functions
///////////////////////////////////////////////////////////////////////////////

func (me *AliyunFSImpl) OpenDir(path string) int {
	return 0
}

func (me *AliyunFSImpl) ReadDir(path string) ([]fscommon.DirItem, int) {

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

	dis := make([]fscommon.DirItem, diCount)

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

		dis[i].Name = fscommon.GetLastPathComp(key) //need remove slash suffix and common prefix (for subdir)
		dis[i].Type = fscommon.S_IFDIR
		dis[i].Size = 0

		//add to cache
		me.addDirCache(key, dis[i])
	}

	//collect files
	j := len(lsRes.CommonPrefixes)
	for i := 0; i < len(lsRes.Objects); i++ {
		key := lsRes.Objects[i].Key
		if key == prefix {
			continue
		}

		dis[j] = fscommon.DirItem{
			Name:  fscommon.GetLastPathComp(key), //need remove common prefix
			Size:  uint64(lsRes.Objects[i].Size),
			Mtime: lsRes.Objects[i].LastModified,
			//lsRes.Objects[i].ETag
			Type: fscommon.S_IFREG,
		}

		//add to cache
		me.addDirCache(key, dis[j])
		j++
	}

	log.Println("Directories and files: ", diCount)
	return dis, diCount
}

func (me *AliyunFSImpl) GetAttr(path string) (*fscommon.DirItem, int) {
	//return fixed info for root path
	if path == "/" {
		return &fscommon.DirItem{
			Name:  "/",
			Mtime: time.Now(),
			Size:  0,
			Type:  fscommon.S_IFDIR,
		}, 0
	}
	if len(path) != 0 && path[0] == '/' {
		path = path[1:]
	}

	//di,ok := me.fileMgr.GetFileInfo(path)
	//if ok {
	//	return di, 0
	//}

	di, ok := me.dirCache.Get(path)
	if ok {
		return di, 0
	}

	if me.notExistCache.Exist(path) {
		return nil, fscommon.ENOENT
	}

	//get attributes from remote
	di, rc := me._getAttrFromRemote(path, fscommon.S_IFUNKOWN)
	if rc == fscommon.ENOENT {
		me.notExistCache.Add(path, &fscommon.DirItem{}, fscommon.CACHE_LIFE_SHORT)
	}
	return di, rc
}

func (me *AliyunFSImpl) NewFileImpl(path string) (fscommon.FileImpl, int) {
	fio := &AliyunIO{
		bucket:     me.bucket,
		fs:         me,
		bucketName: me.bucketName,
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

	fo, ok := me.fileMgr.GetInstance(path)
	if ok == 0 {
		log.Println("Found existing instance for file ", path)
		return fo, 0
	}
	log.Println("Verify existing for file", path)
	//var fileNotExist bool = false

	//verify if the file exists, and if user has permission to open the file in selected mode
	_, ok = me._getAttrFromRemote(path, fscommon.S_IFREG)
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
	fo, ok = me.fileMgr.Allocate(me, path)
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

func (me *AliyunFSImpl) StatFs(name string) (*fscommon.FsInfo, int) {
	return &fscommon.FsInfo{
		Blocks: 1024 * 1024 * 1024,
		Bfree:  1024 * 1024 * 1024,
		Bavail: 1024 * 1024 * 1024,
		Bsize:  1024 * 128,
	}, 0
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
	if me.fileMgr.Exist(path) {
		return fscommon.EBUSY
	}

	err := me.bucket.DeleteObject(path)
	if err != nil {
		log.Println(err)
		return fscommon.EIO
	}
	return 0
}
