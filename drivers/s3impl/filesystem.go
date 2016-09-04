package s3impl

import (
	//"syscall"
	"fmt"
	"time"
	//"strings"
	
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"brightlib.com/common"
)

type S3FileSystemImpl struct {
	awsConfig *aws.Config
	sess      *session.Session
	svc       *s3.S3
	
	bucketName  string
	//dirCache	map[string]fscommon.DirItem
	dirCache   *fscommon.DirCache
	fileMgr	   *fscommon.FileInstanceMgr
}

///////////////////////////////////////////////////////////////////////////////
//Internal functions
///////////////////////////////////////////////////////////////////////////////


func (me *S3FileSystemImpl) addDirCache(key string, di fscommon.DirItem) {
	if key[len(key)-1] == '/' {
		key = key[:len(key)-1]
	}
	me.dirCache.Add(key, &di, fscommon.CACHE_LIFE_SHORT)
}

//trim the last slash if there is
//get the last component of a path
func (me *S3FileSystemImpl) getLastPathComp(path string) string {
	var end int
	if path[len(path)-1] == '/' {
		end = len(path) - 2
	}else{
		end = len(path) - 1
	}
	if end == -1 {		//means path == "/"
		return ""
	}
	idx := 0	//if slash is not found, then we will use the string start from 0
	for i := end; i >= 0; i-- {
		if path[i] == '/' {
			idx = i + 1
			break
		}
	}
	
	fmt.Println("***", path, ": idx=", idx, ", end=", end)
	return path[idx : end + 1]
}

//get attributes for path/file
//it can also be used to check if path/file exists
func (me *S3FileSystemImpl) _getAttrFromRemote(path string, iType int)(*fscommon.DirItem, int) {
	key := path
	if iType == fscommon.S_IFDIR {
		key = key + "/"
	}
	params := &s3.HeadObjectInput{
		Bucket:         aws.String(me.bucketName), // Required
		Key:            aws.String(key),  // Required
	}
	rsp, err := me.svc.HeadObject(params)
	if err != nil {
		// Print the error, cast err to awserr.Error to get the Code and
		// Message from an error.
		if reqerr, ok := err.(awserr.RequestFailure); ok {
			if reqerr.StatusCode()==404 {
				if iType == fscommon.S_IFUNKOWN {
					return me._getAttrFromRemote(key, fscommon.S_IFDIR)
				}else{
					return nil, fscommon.ENOENT
				}
			}
		}
		fmt.Println(err.Error())
		return nil, fscommon.ENOENT		//fail to get attributes
	}
	if iType != fscommon.S_IFDIR {
		iType = fscommon.S_IFREG
	}
	return &fscommon.DirItem{
				Name    : me.getLastPathComp(path),
				Type	: iType,
				Size	: uint64(*rsp.ContentLength), 
				Mtime	: *rsp.LastModified,
				}, 0
}


///////////////////////////////////////////////////////////////////////////////
//Exported functions
///////////////////////////////////////////////////////////////////////////////


func (me *S3FileSystemImpl) OpenDir(path string) (int) {
	return 0
}

func (me *S3FileSystemImpl) ReadDir(path string) ([]fscommon.DirItem , int) {
	
	fmt.Println("S3FileSystemImpl::ReadDir = ", path)
	
	prefix := path
	if len(prefix) != 0 && prefix != "/" {
		prefix = prefix + "/"
	}
	
	params := &s3.ListObjectsV2Input{
		Bucket:            aws.String(me.bucketName), // Required
		//ContinuationToken: aws.String("Token"),
		Delimiter:         aws.String("/"),
		//EncodingType:      aws.String("EncodingType"),
		//FetchOwner:        aws.Bool(true),
		//MaxKeys:           aws.Int64(1),
		Prefix:            aws.String(prefix),
		//StartAfter:        aws.String("StartAfter"),
	}
	rsp, err := me.svc.ListObjectsV2(params)

	if err != nil { 	// resp is not filled
		fmt.Println(err)
		return nil, -1
	}
	
	diCount := len(rsp.CommonPrefixes) + len(rsp.Contents)
	
	dis := make([]fscommon.DirItem,  diCount)
	
	//collect directories
	for i := 0; i<len(rsp.CommonPrefixes); i++ {
		key := *(rsp.CommonPrefixes[i].Prefix)	
		if key == prefix {
			continue
		}
		
		dis[i].Name = me.getLastPathComp(key)	//need remove slash suffix and common prefix (for subdir)
		dis[i].Type = fscommon.S_IFDIR
		dis[i].Size = 0
		
		//add to cache
		me.addDirCache(key, dis[i])
	}
	
	//collect files
	j := len(rsp.CommonPrefixes)	
	for i := 0; i<len(rsp.Contents); i++ {
		key := *(rsp.Contents[i].Key)
		if key == prefix {
			continue
		}
		
		dis[j] = fscommon.DirItem{
					Name : me.getLastPathComp(key),		//need remove common prefix
					Size : uint64(*(rsp.Contents[i].Size)),
					Mtime: *rsp.Contents[i].LastModified,
					//rsp.Contents[i].ETag 
					Type : fscommon.S_IFREG,
				}
		
		//add to cache
		me.addDirCache(key, dis[j])
		j++
	}

	fmt.Println("Directories and files: ", diCount)
	return dis, diCount
}

func (me *S3FileSystemImpl) ReleaseDir() (int) {
	return 0
}

func (me *S3FileSystemImpl) GetAttr(path string)(*fscommon.DirItem, int) {
	di,ok := me.dirCache.Get(path);
	if ok {
		return di, 0
	}else{
		//get attributes from remote
		return me._getAttrFromRemote(path, fscommon.S_IFUNKOWN)
	}
}

func (me *S3FileSystemImpl) NewFileImpl(path string, flags uint32)fscommon.FileImpl {
	fio := &S3FileIO{
		svc			: me.svc,
		bucketName	: me.bucketName,
	}
	return &remoteCache{fileName: path, openFlags: flags, io: fio}
}

func (me *S3FileSystemImpl) Open(path string, flags uint32)(*fscommon.FileObject, int) {
	//look in file instance manager first
	//if successful, this will increase instance reference count
	fo,ok := me.fileMgr.GetInstance(path, flags)
	if ok==0 {
		return fo, 0
	}
	
	//verify if the file exists, and if user has permission to open the file in selected mode
	_, ok = me._getAttrFromRemote(path, fscommon.S_IFREG)
	switch ok {
		case fscommon.EIO:
			return nil,ok
		case fscommon.ENOENT:
			if (flags&fscommon.O_CREAT==0) {
				return nil,ok
			}
		default:
			return nil,ok
	}
	
	fo,ok = me.fileMgr.Allocate(me, path, flags)
	return fo, ok
}

func (me *S3FileSystemImpl) Chmod(name string, mode uint32)(int) {
	return 0
}

func (me *S3FileSystemImpl) Utimens(name string, Mtime *time.Time)(int) {
	return 0
}

func (me *S3FileSystemImpl) StatFs(name string)(*fscommon.FsInfo, int) {
	return &fscommon.FsInfo{
		Blocks  : 1024 * 1024 * 1024,
		Bfree   : 1024 * 1024 * 1024,
		Bavail  : 1024 * 1024 * 1024,
		Bsize   : 1024 * 128,
	}, 0
}

func (me *S3FileSystemImpl) Mkdir(path string, mode uint32)(int) {
	//check parent folder exist
	//_,ok := me._getAttrFromRemote(parpath, S_IFDIR)
	//if ok != 0 {
	//	return -1
	//}
	
	//create the folder
	key := path
	if path[len(path)-1] != '/' {
		key = path + "/"
	}
	var length int64 = 0
	params := &s3.PutObjectInput{
		Bucket	: aws.String(me.bucketName),
		Key		: aws.String(key),
		ContentLength : &length,
	}
	_,err := me.svc.PutObject(params)
	if err != nil {
		return -1
	}
	return 0
}

func (me *S3FileSystemImpl) Unlink(path string)(int) {
	//check if it's a file. we only deal with file here
	if path[len(path)-1] == '/' {
		return -1
	}
	
	//if the file is being open, return status busy
	if me.fileMgr.Exist(path) {
		return fscommon.EBUSY
	}
	
	//delete the file
	params := &s3.DeleteObjectInput{
		Bucket:       aws.String(me.bucketName), // Required
		Key:          aws.String(path),  // Required
	}
	_, err := me.svc.DeleteObject(params)
	
	//remove dir cache if there is
	//remove it even previous step gets failed. just to force a refresh when access it next time
	me.dirCache.Remove(path)
	
	if err != nil {
		return -1
	}
	
	return 0
}
