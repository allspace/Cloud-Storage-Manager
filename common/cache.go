package fscommon

import(
	"time"
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

