package fscommon

import (
	"log"
	"os"
	"sync"
	"time"
)

type FSImplBase struct {
	BucketName    string
	DirCache      *DirCache
	NotExistCache *DirCache //avoid flag files being checked too frequently
	FileMgr       *FileInstanceMgr
}

func (me *FSImplBase) Init(bucketName string) {
	me.BucketName = bucketName
	me.DirCache = NewDirCache()
	me.NotExistCache = NewDirCache()
	me.FileMgr = NewFileInstanceMgr()
}

func (me *FSImplBase) StatFs(name string) (*FsInfo, int) {
	return &FsInfo{
		Blocks: 1024 * 1024 * 1024,
		Bfree:  1024 * 1024 * 1024,
		Bavail: 1024 * 1024 * 1024,
		Bsize:  1024 * 128,
	}, 0
}

func (me *FSImplBase) GetAttr(path string) (os.FileInfo, int) {
	//return fixed info for root path
	if path == "/" {
		return &DirItem{
			DiName:  "/",
			DiMtime: time.Now(),
			DiSize:  0,
			DiType:  S_IFDIR,
		}, 0
	}

	//di,ok := me.fileMgr.GetFileInfo(path)
	//if ok {
	//	return di, 0
	//}

	di, ok := me.DirCache.Get(path)
	if ok {
		return di, 0
	}

	if me.NotExistCache.Exist(path) {
		return nil, ENOENT
	}

	return nil, 0
}

///////////////////////////////////////////////////////////////////////////////

type FileImplBase struct {
	FileName  string
	FileLen   int64
	OpenFlags uint32

	File         ISliceFile
	AppendBuffer *CacheBuffer
	ReadBuffer   *CacheBuffer
	buffAllocMtx sync.Mutex

	Modified bool
}

//no IO should be involved in this release function
func (me *FileImplBase) Release() int {
	return 0
}

func (me *FileImplBase) Utimens(Mtime *time.Time) int {
	return 0
}

func (me *FileImplBase) Truncate(size uint64) int {
	ok := me.File.Truncate(size)
	if ok < 0 {
		return ok
	}
	me.FileLen = me.File.GetLength()
	if me.FileLen == 0 {
		me.AppendBuffer.ResetOffset(me.File.GetLength())
	}
	return 0
}

func (me *FileImplBase) Append(data []byte, offset int64) int {
	if me.AppendBuffer == nil {
		me.buffAllocMtx.Lock()
		if me.AppendBuffer == nil {
			me.AppendBuffer = NewCacheBuffer(0, nil, 0)
		}
		me.buffAllocMtx.Unlock()
	}

	return me.AppendBuffer.Write(data, offset)
}

func (me *FileImplBase) GetInfo() os.FileInfo {
	return &DirItem{
		DiName:  me.FileName,
		DiSize:  me.FileLen,
		DiMtime: time.Now(),
		DiType:  S_IFREG,
	}
}

func (me *FileImplBase) Read(dest []byte, offset int64) int {

	remainLen := len(dest)
	curOffset := offset
	curDest := dest

	log.Printf("FileImplBase::Read %s is called: offset = %d, length = %d.", me.FileName, offset, len(dest))
	//log.Printf("File length: %d", me.File.GetLength())
	//offset falls into base file, or even later
	if curOffset < me.File.GetLength() {
		n := me.bufferRead(curDest, curOffset)
		//n := me.File.Read(curDest, curOffset)
		if n < 0 {
			log.Printf("me.File.Read returns %d", n)
			return n
		}
		remainLen -= n
		curOffset += int64(n)
		curDest = curDest[n:]
	}

	//offset falls into append buffer scope
	if remainLen > 0 && me.AppendBuffer != nil && curOffset >= me.AppendBuffer.BaseOffset {
		log.Printf("Read append buffer: curOffset=%d, remainLen=%d", curOffset, remainLen)

		n := me.AppendBuffer.Read(curDest, curOffset)
		remainLen -= n
		curOffset += int64(n)
		curDest = curDest[n:]
	}

	log.Printf("Read returns data length: %d", (len(dest) - remainLen))
	return (len(dest) - remainLen)
}

func (me *FileImplBase) bufferRead(dest []byte, offset int64) int {
	log.Printf("FileImplBase::bufferRead: offset %d", offset)

	//allocate buffer if not yet
	if me.ReadBuffer == nil {
		me.buffAllocMtx.Lock()
		if me.ReadBuffer == nil {
			me.ReadBuffer = NewCacheBuffer(-1, nil, 1024*1024)
		}
		me.buffAllocMtx.Unlock()
	}

	//remeber data pointers
	remainLen := len(dest)
	curOffset := offset
	curDest := dest

	//try to read from buffer
read_buffer:
	if curOffset >= me.ReadBuffer.BaseOffset && curOffset < me.ReadBuffer.MaxOffset {
		var start int = int(curOffset - me.ReadBuffer.BaseOffset)
		var count int
		if int(me.ReadBuffer.MaxOffset-curOffset) > remainLen {
			count = remainLen
		} else {
			count = int(me.ReadBuffer.MaxOffset - curOffset)
		}
		src := me.ReadBuffer.Buffer[start : start+count]
		copy(curDest, src)
		remainLen -= count
		curOffset += int64(count)
		curDest = curDest[count:]
	}

	//load data into buffer
	if remainLen > 0 {
		log.Printf("FileImplBase::bufferRead reads data from remote %d.", len(me.ReadBuffer.Buffer))
		//need sync
		me.ReadBuffer.mtx.Lock()
		n := me.File.Read(me.ReadBuffer.Buffer, curOffset)
		me.ReadBuffer.mtx.Unlock()
		//log.Printf("me.File.Read returns offset %d length %d", curOffset, n)
		if n < 0 {

			if remainLen == len(dest) {
				return n
			}
		} else if n == 0 {
			return len(dest) - remainLen
		}

		me.ReadBuffer.BaseOffset = curOffset
		me.ReadBuffer.MaxOffset = curOffset + int64(n)

		goto read_buffer
	}

	//log.Printf("FileImplBase::bufferRead returns data length %d", len(dest)-remainLen)
	return len(dest) - remainLen
}
