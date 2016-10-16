package s3impl

import (
	//"syscall"
	"fmt"
	"os"
	"time"
	//"strings"
	"log"

	"github.com/allspace/csmgr/common"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type S3FileSystemImpl struct {
	awsConfig *aws.Config
	sess      *session.Session
	svc       *s3.S3

	bucketName string
	//dirCache	map[string]fscommon.DirItem
	dirCache *fscommon.DirCache
	fileMgr  *fscommon.FileInstanceMgr
}

///////////////////////////////////////////////////////////////////////////////
//Internal functions
///////////////////////////////////////////////////////////////////////////////

func (me *S3FileSystemImpl) addDirCache(key string, di *fscommon.DirItem) {
	if key[len(key)-1] == '/' {
		key = key[:len(key)-1]
	}
	me.dirCache.Add(key, di, fscommon.CACHE_LIFE_SHORT)
}

//get attributes for path/file
//it can also be used to check if path/file exists
func (me *S3FileSystemImpl) _getAttrFromRemote(path string, iType int) (os.FileInfo, int) {
	key := path
	if iType == fscommon.S_IFDIR {
		key = key + "/"
	}
	params := &s3.HeadObjectInput{
		Bucket: aws.String(me.bucketName), // Required
		Key:    aws.String(key),           // Required
	}
	rsp, err := me.svc.HeadObject(params)
	if err != nil {
		// Print the error, cast err to awserr.Error to get the Code and
		// Message from an error.
		if reqerr, ok := err.(awserr.RequestFailure); ok {
			if reqerr.StatusCode() == 404 {
				if iType == fscommon.S_IFUNKOWN {
					return me._getAttrFromRemote(key, fscommon.S_IFDIR)
				} else {
					return nil, fscommon.ENOENT
				}
			}
		}
		fmt.Println(err.Error())
		return nil, fscommon.ENOENT //fail to get attributes
	}
	if iType != fscommon.S_IFDIR {
		iType = fscommon.S_IFREG
	}
	return &fscommon.DirItem{
		DiName:  fscommon.GetLastPathComp(path),
		DiType:  iType,
		DiSize:  *rsp.ContentLength,
		DiMtime: *rsp.LastModified,
	}, 0
}

///////////////////////////////////////////////////////////////////////////////
//Exported functions
///////////////////////////////////////////////////////////////////////////////

func (me *S3FileSystemImpl) OpenDir(path string) int {
	return 0
}

func (me *S3FileSystemImpl) ReadDir(path string) ([]os.FileInfo, int) {

	fmt.Println("S3FileSystemImpl::ReadDir = ", path)

	prefix := path
	if len(prefix) != 0 && prefix != "/" {
		prefix = prefix + "/"
	}

	params := &s3.ListObjectsV2Input{
		Bucket: aws.String(me.bucketName), // Required
		//ContinuationToken: aws.String("Token"),
		Delimiter: aws.String("/"),
		//EncodingType:      aws.String("EncodingType"),
		//FetchOwner:        aws.Bool(true),
		//MaxKeys:           aws.Int64(1),
		Prefix: aws.String(prefix),
		//StartAfter:        aws.String("StartAfter"),
	}
	rsp, err := me.svc.ListObjectsV2(params)

	if err != nil { // resp is not filled
		fmt.Println(err)
		return nil, -1
	}

	diCount := len(rsp.CommonPrefixes) + len(rsp.Contents)

	dis := make([]os.FileInfo, diCount)

	//collect directories
	for i := 0; i < len(rsp.CommonPrefixes); i++ {
		key := *(rsp.CommonPrefixes[i].Prefix)
		if key == prefix {
			continue
		}
		//hide cache/tmp dir or slice group
		if key[0] == '$' && key[len(key)-1] == '$' {
			continue
		}

		di := &fscommon.DirItem{
			DiName: fscommon.GetLastPathComp(key), //need remove slash suffix and common prefix (for subdir)
			DiType: fscommon.S_IFDIR,
			DiSize: 0,
		}
		dis[i] = di

		//add to cache
		me.addDirCache(key, di)
	}

	//collect files
	j := len(rsp.CommonPrefixes)
	for i := 0; i < len(rsp.Contents); i++ {
		key := *(rsp.Contents[i].Key)
		if key == prefix {
			continue
		}

		di := &fscommon.DirItem{
			DiName:  fscommon.GetLastPathComp(key), //need remove common prefix
			DiSize:  *(rsp.Contents[i].Size),
			DiMtime: *rsp.Contents[i].LastModified,
			//rsp.Contents[i].ETag
			DiType: fscommon.S_IFREG,
		}
		dis[j] = di
		//add to cache
		me.addDirCache(key, di)
		j++
	}

	fmt.Println("Directories and files: ", diCount)
	return dis, diCount
}

func (me *S3FileSystemImpl) ReleaseDir() int {
	return 0
}

func (me *S3FileSystemImpl) GetAttr(path string) (os.FileInfo, int) {
	di, ok := me.fileMgr.GetFileInfo(path)
	if ok {
		return di, 0
	}

	di, ok = me.dirCache.Get(path)
	if ok {
		return di, 0
	}

	//get attributes from remote
	return me._getAttrFromRemote(path, fscommon.S_IFUNKOWN)
}

//this function runs in big lock context
func (me *S3FileSystemImpl) NewFileImpl(path string) (fscommon.FileImpl, int) {

	fio := &S3FileIO{
		svc:        me.svc,
		fs:         me,
		bucketName: me.bucketName,
	}

	rc := newRemoteCache(path, fio, me)
	return rc, 0
	//return &remoteCache{fileName: path, openFlags: flags, io: fio}
}

func (me *S3FileSystemImpl) Open(path string, flags uint32) (*fscommon.FileObject, int) {
	//look in file instance manager first
	//if successful, this will increase instance reference count
	log.Println("Try to find existing object for file", path)

	fo, ok := me.fileMgr.GetInstance(path)
	if ok == 0 {
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

func (me *S3FileSystemImpl) Chmod(name string, mode uint32) int {
	return 0
}

func (me *S3FileSystemImpl) Utimens(name string, Mtime *time.Time) int {
	return 0
}

func (me *S3FileSystemImpl) StatFs(name string) (*fscommon.FsInfo, int) {
	return &fscommon.FsInfo{
		Blocks: 1024 * 1024 * 1024,
		Bfree:  1024 * 1024 * 1024,
		Bavail: 1024 * 1024 * 1024,
		Bsize:  1024 * 128,
	}, 0
}

func (me *S3FileSystemImpl) Mkdir(path string, mode uint32) int {
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
		Bucket:        aws.String(me.bucketName),
		Key:           aws.String(key),
		ContentLength: &length,
	}
	_, err := me.svc.PutObject(params)
	if err != nil {
		log.Println(err.Error())
		return fscommon.EIO
	}
	return 0
}

func (me *S3FileSystemImpl) Unlink(path string) int {
	//check if it's a file. we only deal with file here
	if path[len(path)-1] == '/' {
		return fscommon.EINVAL
	}

	//if the file is being open, return status busy
	if me.fileMgr.Exist(path) {
		return fscommon.EBUSY
	}

	//delete the file
	params := &s3.DeleteObjectInput{
		Bucket: aws.String(me.bucketName), // Required
		Key:    aws.String(path),          // Required
	}
	_, err := me.svc.DeleteObject(params)

	//remove dir cache if there is
	//remove it even previous step gets failed. just to force a refresh when access it next time
	me.dirCache.Remove(path)

	if err != nil {
		log.Println(err.Error())
		return fscommon.EIO
	}

	return 0
}
