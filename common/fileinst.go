package fscommon
import (
	"log"
	"runtime"
	"sync"
)

type FileInstance struct {
	file FileImpl
	refCount int
	wlock *FileObject
	rlock int
	mtx sync.Mutex
}

type FileInstanceMgr struct {
	fileInstList map[string]*FileInstance
	mtx sync.Mutex
}

//one-to-one relationship with user space file handle
type FileObject struct {
	fileMgr     *FileInstanceMgr
	fileInst 	*FileInstance
	fileName 	string
	openFlags 	uint32
}

func NewFileInstanceMgr() *FileInstanceMgr {
	return &FileInstanceMgr {
		fileInstList	: make(map[string]*FileInstance),
	}
}

func (me *FileInstanceMgr) GetInstance(name string, flags uint32)(*FileObject,int) {
	me.mtx.Lock()
	defer me.mtx.Unlock()
	
	//find an existing instance
	fi,ok := me.fileInstList[name]
	if ok==true {
		fi.refCount++
		return &FileObject{
			fileMgr		: me,
			fileInst	: fi,
			fileName	: name,
			openFlags   : flags,
		},0
	}
	
	return nil, ENOENT
}

func (me *FileInstanceMgr) Allocate(fs FileSystemImpl, name string, flags uint32)(*FileObject,int) {
	me.mtx.Lock()
	defer me.mtx.Unlock()
	
	//find an existing instance
	fi,ok := me.fileInstList[name]
	if ok==true {
		fi.refCount++
		return &FileObject{
			fileMgr		: me,
			fileInst	: fi,
			fileName	: name,
			openFlags   : flags,
		},0
	}
	
	//not found? create new instance
	fi = &FileInstance {
		refCount	: 1,
	}
	me.fileInstList[name] = fi
	fi.file = fs.NewFileImpl(name, flags)
	
	return &FileObject{
		fileMgr		: me,
		fileInst	: fi,
		fileName	: name,
		openFlags   : flags,
	},0
}

func (me *FileInstanceMgr) Release(name string)int {
	me.mtx.Lock()
	// we do not use "defer Unlock()" here because we will run GC which may block for some time
	
	fi,ok := me.fileInstList[name]
	if ok==false {
		me.mtx.Unlock()
		return -1
	}
	
	var needGC bool = false
	fi.refCount--
	if fi.refCount==0 {
		delete(me.fileInstList, name)
		needGC = true
	}
	me.mtx.Unlock()
	
	if needGC {
		runtime.GC()		//run a GC here to free memory of file instance
	}
	return 0
}

func (me *FileInstanceMgr) Exist(name string)bool {
	me.mtx.Lock()
	defer me.mtx.Unlock()
	
	_,ok := me.fileInstList[name]
	if ok {
		return true
	}else{
		return false
	}
}


func (me *FileInstance) TryGetWLock(fo *FileObject)bool {
	me.mtx.Lock()
	defer me.mtx.Unlock()
	
	if me.wlock==fo {
		return true
	}
	if me.wlock==nil {
		me.wlock = fo
		return true
	}
	
	return false
}

func (me *FileInstance) ReleaseWLock(fo *FileObject) {
	me.mtx.Lock()
	defer me.mtx.Unlock()
	
	me.wlock = nil
}

///////////////////////////////////////////////////////////////////////////////
//File Object
//one-to-one relationship with user space file handle
//multiple file objects to one file implimentation
///////////////////////////////////////////////////////////////////////////////

func (me *FileObject) Release() {
	me.fileInst.ReleaseWLock(me)		//maybe we should release lock in function flush which is called by close
	me.fileMgr.Release(me.fileName)
	me.fileInst = nil
}

func (me *FileObject) Read(data []byte, offset int64)(int) {
	if me.fileInst == nil {
		return -1
	}
	return me.fileInst.file.Read(data, offset)
}

func (me *FileObject) Write(data []byte, offset int64)(int) {
	if me.fileInst == nil {
		return -1
	}
	
	//only one client can hold write access to a file
	//so, we need try to get a write lock here
	//this test is deferred so that clients which open file with wrong flags can still work
	rc := me.fileInst.TryGetWLock(me)
	if rc==false {
		log.Println("Failed to get write lock.")
		return -1 //permission denined
	}
	
	return me.fileInst.file.Write(data, offset)
}

func (me *FileObject) Flush()(int) {
	return me.fileInst.file.Flush()
}
