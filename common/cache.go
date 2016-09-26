package fscommon

import(
	"time"
	"log"
)

const (
	CACHE_LIFE_ONE	 = 1
	CACHE_LIFE_SHORT = 10
	CACHE_LIFE_LONG	 = 9999999
)

type dirCacheItem struct {
	item DirItem
	tmsp int64
	life int
}

type DirCache struct {
	dcache map[string]dirCacheItem
}

func NewDirCache() *DirCache {
	return &DirCache{ dcache : make(map[string]dirCacheItem) }
}

func (me *DirCache) Add(key string, di *DirItem, lf int) {
	me.dcache[key] = dirCacheItem{
		item	:	*di,
		tmsp	:	time.Now().Unix(),
		life	:	lf,
	}
}

func (me *DirCache) Remove(key string) {
	delete(me.dcache, key)
}

func (me *DirCache) Get(key string) (*DirItem,bool) {
	it,ok := me.dcache[key]
	if ok==false {
		return nil, ok
	}
	switch it.life {
		case CACHE_LIFE_ONE:
		{
			delete(me.dcache, key)
			return &(it.item), true
		}
		case CACHE_LIFE_SHORT:
		{
			n := time.Now().Unix() - it.tmsp
			if n>10 || n< -10 {
				delete(me.dcache, key)
				return nil, false
			}
			return &(it.item), true
		}
		case CACHE_LIFE_LONG:
		{
			return &(it.item), true
		}
	}
	
	return nil,false
}

func (me *DirCache) Exist(key string) bool{
	it, ok := me.dcache[key]
	
	n := time.Now().Unix() - it.tmsp
	if n>CACHE_LIFE_SHORT || n< -(CACHE_LIFE_SHORT) {
		delete(me.dcache, key)
		return false
	}
	
	return ok
}

///////////////////////////////////////////////////////////////////////////////
type CacheFullEvent func(data []byte, offset int64)(int)

type CacheBuffer struct {
	Buffer 		[]byte
	BufferLen 	int
	BaseOffset 	int64
	FullOffset  int64
	MaxOffset   int64
	
	distWriteList map[int64]int
	EvtOnFull CacheFullEvent
}

func NewCacheBuffer(offset int64, handler CacheFullEvent, bsize int) *CacheBuffer {
	if bsize == 0 {
	    bsize = FILE_BLOCK_SIZE + 1024 * 1024		//6 MB
	}
	return &CacheBuffer{
		Buffer 		: make([]byte, bsize),
		BufferLen 	: bsize,
		BaseOffset	: offset,			//file offset for the start position of the buffer
		FullOffset  : offset,			//file offset at the postion before which is all filled data
		MaxOffset	: offset,			//file offset at the max position where data has been filled
		
		EvtOnFull   : handler,
	}
}

func (me *CacheBuffer) ResetOffset(offset int64) {
    me.BaseOffset = offset
	me.FullOffset = offset
	me.MaxOffset  = offset
}

func (me *CacheBuffer) GetDataLen()int {
	return int(me.MaxOffset - me.BaseOffset)
}

func (me *CacheBuffer) GetData()[]byte {
    return me.Buffer[0: int(me.MaxOffset - me.BaseOffset)]
}

func (me *CacheBuffer) Read(data []byte, offset int64)int {
	curOffset := offset
	remainLen := len(data)
	curDest   := data
	var n int = 0
	
	if remainLen > 0 && curOffset >= me.BaseOffset {
		start := int(curOffset - me.BaseOffset)
		
		if me.MaxOffset - curOffset >= int64(len(curDest)) {
			n = len(curDest)
		}else{
			n = int(me.MaxOffset - curOffset)
		}
		copy(curDest, me.Buffer[start: start + n])
		//remainLen -= n
		//curOffset += int64(n)
	}
	
	return n
}

func (me *CacheBuffer) Write(data []byte, offset int64)int {
	var blkCount int = 0
	
	remainN := len(data)
	curData := data
	curOffset := me.BaseOffset
	
	for remainN > 0 {
		//save data to buffer first
		start := int(curOffset - me.BaseOffset)
		freeN := me.BufferLen - start
		if freeN <= 0 {
			log.Printf("There must be something wrong: BaseOffset = %d\n", curOffset)
			return EIO
		}
		copyN := 0
		if freeN > remainN {
			copyN = remainN
		} else {
			copyN = freeN
		}
		
		buff := me.Buffer[start:]
		copy(buff, curData[0:copyN])
		
		//let's see if we can move FullOffset forward
		n := curOffset + int64(copyN) - me.FullOffset
		if curOffset <= me.FullOffset && n > 0 {
			me.FullOffset += n
			
			//FullOffset get moved, check list
			for off,m := range me.distWriteList {
				if off <= me.FullOffset {
					if off + int64(m) > me.FullOffset {
						me.FullOffset += (off + int64(m) - me.FullOffset)
					} 
					delete(me.distWriteList, off)
				}
			}
		
		}else{
			me.distWriteList[curOffset] = copyN			//discontinous write, add to list
		}
		
		if curOffset + int64(copyN) > me.MaxOffset {
			me.MaxOffset = curOffset + int64(copyN)
		}
				
		curOffset += int64(copyN)
		remainN -= copyN
		curData = curData[copyN:]
		//me.fileLen = me.MaxOffset	//file length
		
		//buffer data length reach FILE_BLOCK_SIZE, trigger T1 block uploading
		dLen := me.FullOffset - me.BaseOffset
		if dLen >= FILE_BLOCK_SIZE {
			
			//upload the buffer
			//name := me.file.GetCacheBlockFileName(me.BaseOffset)
			//ok := me.io.PutBuffer(name, me.Buffer[0:dLen])
			ok := me.EvtOnFull(me.Buffer[0:dLen], me.BaseOffset)
			if ok < 0 {		//we cannot move forward if buffer cannot be uploaded
				log.Println("Failed to execute EvtOnFull handler.")
				return ok
			}
			
			me.BaseOffset += FILE_BLOCK_SIZE
			blkCount += 1
			//TODO: clean unused buffer area
			copy(me.Buffer, me.Buffer[dLen:])
		}
	}
	
	return blkCount
}
