package aliyunimpl

import (
    //"os"
	"time"
    "log"
	"strconv"
    "github.com/aliyun/aliyun-oss-go-sdk/oss"
	
	"brightlib.com/common"
)

type AliyunFSImpl struct {
    client *oss.Client
    bucket *oss.Bucket
	
	bucketName string
	dirCache   *fscommon.DirCache
	fileMgr	   *fscommon.FileInstanceMgr
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
func (me *AliyunFSImpl) _getAttrFromRemote(path string, iType int)(*fscommon.DirItem, int) {
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
				}else{
					return nil, fscommon.ENOENT
				}
			}
		}
		log.Println(err.Error())
		return nil, fscommon.ENOENT		//fail to get attributes
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
    mtime, _ := time.Parse(longForm, meta["LastModified"][0])
	return &fscommon.DirItem{
				Name    : fscommon.GetLastPathComp(path),
				Type	: iType,
				Size	: uint64(size), 
				Mtime	: mtime,
				}, 0
}


///////////////////////////////////////////////////////////////////////////////
//Exported functions
///////////////////////////////////////////////////////////////////////////////



func (me *AliyunFSImpl) PutFile(tgtName string, path string)int {
    err := me.bucket.PutObjectFromFile(tgtName, path)
    if err != nil {
        log.Println(err)
        return -5 //EIO
    }
    return 0
}

func (me *AliyunFSImpl) ListDir(path string)int {
    lsRes, err := me.bucket.ListObjects()
    if err != nil {
        log.Println(err)
        return -5 //EIO
    }

    for _, object := range lsRes.Objects {
        log.Println("Objects:", object.Key)
    }

    return 0
}

func (me *AliyunFSImpl) OpenDir(path string) (int) {
	return 0
}

func (me *AliyunFSImpl) ReadDir(path string) ([]fscommon.DirItem , int) {
	
	log.Println("AliyunFSImpl::ReadDir = ", path)
	
	prefix := path
	if len(prefix) != 0 && prefix[0] == '/' {
		prefix = prefix[1:]
	}
	var lsRes oss.ListObjectsResult
	var err error
	if len(prefix) == 0 {
		lsRes, err = me.bucket.ListObjects()
	}else{
		lsRes, err = me.bucket.ListObjects(oss.Prefix(prefix), oss.Delimiter("/"))
	}
	if err != nil {
		log.Println(err)
		return nil, fscommon.EIO
	}
	
	diCount := len(lsRes.CommonPrefixes) + len(lsRes.Objects)
	
	dis := make([]fscommon.DirItem,  diCount)
	
	//collect directories
	for i := 0; i<len(lsRes.CommonPrefixes); i++ {
		key := lsRes.CommonPrefixes[i]
		if key == prefix {
			continue
		}
		//hide cache/tmp dir or slice group
		if key[0] == '$' && key[len(key)-1] == '$' {
			continue
		}
		
		dis[i].Name = fscommon.GetLastPathComp(key)	//need remove slash suffix and common prefix (for subdir)
		dis[i].Type = fscommon.S_IFDIR
		dis[i].Size = 0
		
		//add to cache
		me.addDirCache(key, dis[i])
	}
	
	//collect files
	j := len(lsRes.CommonPrefixes)	
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
		
		//add to cache
		me.addDirCache(key, dis[j])
		j++
	}

	log.Println("Directories and files: ", diCount)
	return dis, diCount
}


func (me *AliyunFSImpl) GetAttr(path string)(*fscommon.DirItem, int) {
	di,ok := me.fileMgr.GetFileInfo(path)
	if ok {
		return di, 0
	}
	
	di,ok = me.dirCache.Get(path);
	if ok {
		return di, 0
	}
	
	//get attributes from remote
	return me._getAttrFromRemote(path, fscommon.S_IFUNKOWN)
}

func (me *AliyunFSImpl) NewFileImpl(path string)(fscommon.FileImpl, int) {
    return nil,0
}

func (me *AliyunFSImpl) Open(path string, flags uint32)(*fscommon.FileObject, int) {
    return nil,0
}

func (me *AliyunFSImpl) Chmod(name string, mode uint32)(int) {
	return 0
}

func (me *AliyunFSImpl) Utimens(name string, Mtime *time.Time)(int) {
	return 0
}

func (me *AliyunFSImpl) StatFs(name string)(*fscommon.FsInfo, int) {
	return &fscommon.FsInfo{
		Blocks  : 1024 * 1024 * 1024,
		Bfree   : 1024 * 1024 * 1024,
		Bavail  : 1024 * 1024 * 1024,
		Bsize   : 1024 * 128,
	}, 0
}

func (me *AliyunFSImpl) Mkdir(path string, mode uint32)(int) {
    return 0
}

func (me *AliyunFSImpl) Unlink(path string)(int) {
	//check if it's a file. we only deal with file here
	if path[len(path)-1] == '/' {
		return fscommon.EINVAL
	}
	
	//if the file is being open, return status busy
	if me.fileMgr.Exist(path) {
		return fscommon.EBUSY
	}
	
	return 0
}


