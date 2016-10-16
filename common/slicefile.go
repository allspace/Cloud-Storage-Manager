package fscommon

import (
	"encoding/json"
	"fmt"
	"log"
)

const (
	FILE_BLOCK_SIZE = 5 * 1024 * 1024
	FILE_SLICE_SIZE = 5 * 1024 * 1024 * 1024
)

type ISliceFile interface {
	GetCacheBlockFileName(blkId int64) string
	Open(path string, flags uint32) int
	GetLength() int64
	SaveMeta() int
	Truncate(size uint64) int
	Read(data []byte, offset int64) int
	Append(blocks []int64, data []byte) int
}

type SliceMeta struct {
	SliceSize        int64
	SliceCount       int64
	CurSliceFileName string
	CurSliceFileLen  int64
	FileLen          int64 //file length, include committed blocks only
}

type SliceFile struct {
	io FileIO

	FileName     string
	metaFileName string
	meta         SliceMeta
	isSlicedFile bool
}

func (me *SliceMeta) AppendLength(aLen int64) {
	me.CurSliceFileLen += aLen
	me.FileLen += aLen
}

func (me *SliceMeta) IncSliceCount(count int) {
	me.SliceCount += int64(count)
}

func (me *SliceMeta) SetSliceCount(count int64) {
	me.SliceCount = count
}

func (me *SliceFile) SetIO(io FileIO) {
	me.io = io
}

func (me *SliceFile) GetCacheBlockFileName(blkId int64) string {
	return fmt.Sprintf("/$cache$/%s/blocks/%d", me.FileName, blkId)
}

func (me *SliceFile) GetLength() int64 {
	return me.meta.FileLen
}

func (me *SliceFile) Open(path string, flags uint32) int {
	me.FileName = path
	me.metaFileName = fmt.Sprintf("/$slice$/%s/meta", path)

	ok := me.tryRecovery(path)
	if ok < 0 {
		log.Println("Unable recover file ", path)
		return ok
	}

	if me.isSlicedFile == false {
		me.meta.CurSliceFileName = me.FileName
		var flen int64
		di, rc := me.io.GetAttr(me.meta.CurSliceFileName)
		if rc < 0 {
			if rc != ENOENT {
				return rc
			} else {
				if (flags & O_CREAT) == 0 {
					return ENOENT
				} else {
					rc := me.io.PutBuffer(path, nil)
					if rc < 0 {
						return rc
					}
				}
				flen = 0
			}
		} else {
			flen = di.Size()
		}
		me.meta.CurSliceFileLen = flen
		me.meta.FileLen = flen
		me.meta.SliceSize = FILE_SLICE_SIZE
	}

	return 0
}

//it's possible there is data inconsistency (e.g. fs crash) during last mount
//in that case we need do some recovery now
func (me *SliceFile) tryRecovery(path string) int {
	var needUpdate bool = false

	//check if path is a slice file by check its meta data
	ok := me.loadMeta()

	//before extend a normal file to slice file, we will "touch" a meta data file first.
	//this guarantees meta data file always exists, even it's not updated
	//no meta data file, not a slice file, no need recover
	if ok == ENOENT {
		me.isSlicedFile = false
		log.Printf("%s is not a sliced file.\n", path)
		return 0
	}
	//other failure, cannot go further
	if ok < 0 {
		return ok
	}

	//at this step, we are sure this is a sliced file
	me.isSlicedFile = true

	if me.meta.CurSliceFileName != path {
		log.Printf("Slice meta data mismatch: file name = %s, current slice name = %s\n", path, me.meta.CurSliceFileName)
		return -1
	}

	//check if me.meta.CurSliceFileLen is updated
	di, _ := me.io.GetAttr(me.meta.CurSliceFileName)
	size := di.Size()
	if size != me.meta.CurSliceFileLen {
		me.meta.AppendLength(size - me.meta.CurSliceFileLen)
		needUpdate = true
	}
	//check if SliceCount/FileLen is updated
	//load slice list first
	dir := fmt.Sprintf("$slice$/%s", path)
	dis, ok := me.io.ListFile(dir)
	if ok < 0 {
		return ok
	}
	var count int64 = 0
	for _, di := range dis {
		if di.Name() == "meta" {
			continue
		}
		if di.Size() != me.meta.SliceSize {
			log.Printf("Mismatched file slice size: file %s's size: %d, expected size: %d", di.Name, di.Size, me.meta.SliceSize)
			return -1
		}
		count++
	}

	//this means meta data was not updated after a new formal slice was created
	if me.meta.SliceCount != count {
		me.meta.SetSliceCount(count)
		needUpdate = true
	}

	size = count*me.meta.SliceSize + me.meta.CurSliceFileLen
	if size > me.meta.FileLen {
		//we are sure that all formal slices are valid, but we are not sure if current slice is valid
		//we have to discard it
		me.meta.FileLen = count * me.meta.SliceSize
		ok = me.io.ZeroFile(me.meta.CurSliceFileName)
		if ok != 0 {
			return ok
		}
		needUpdate = true
	} else if size < me.meta.FileLen {
		//don't why, but let's just update correct value
		me.meta.FileLen = size
		needUpdate = true
	}

	//save meta data file if something got corrected
	if needUpdate {
		return me.SaveMeta()
	}

	return 0
}

//load slice file meta data
//if there is only one slice (normal file), there won't be meta data file
func (me *SliceFile) loadMeta() int {
	data := make([]byte, 1024)
	ok := me.io.GetBuffer(me.metaFileName, data, 0)
	if ok <= 2 {
		return ok
	}
	if data[0] != '\x01' || data[1] != '\x00' {
		return -1
	}
	err := json.Unmarshal(data[2:], me.meta)
	if err != nil {
		log.Println("error: ", err)
		return -1
	}
	return 0
}

//save slice file meta data
func (me *SliceFile) SaveMeta() int {
	//no need meta data file when no full slice
	if me.meta.SliceCount == 0 {
		return 0
	}

	b, err := json.Marshal(me.meta)
	if err != nil {
		log.Println("error:", err)
		return -1
	}
	data := make([]byte, len(b)+2)
	copy(data[2:], b)
	data[0] = '\x01'
	data[1] = '\x00'
	ok := me.io.PutBuffer(me.metaFileName, data)
	return ok
}

func (me *SliceFile) Truncate(size uint64) int {
	if size == 0 {
		ok := me.io.PutBuffer(me.FileName, nil)
		if ok < 0 {
			return ok
		}
		me.meta.FileLen = 0
		me.meta.CurSliceFileLen = 0
		me.meta.SliceCount = 0

		//TODO: unlink all slice files
		me.io.Unlink(me.metaFileName)
	}
	return 0
}

func (me *SliceFile) Read(data []byte, offset int64) int {
	if offset >= me.meta.FileLen {
		return 0 //EOF
	}

	//case #1: there is no addtional full slice
	log.Printf("me.meta.SliceCount=%d", me.meta.SliceCount)
	if me.meta.SliceCount == 0 {
		return me.io.GetBuffer(me.FileName, data, offset)
	}

	//case #2: there is at least one full slice
	sliceNum := int64(offset / me.meta.SliceSize)
	sliceOffset := offset % me.meta.SliceSize
	reqLen := len(data)
	var n int64 = 0
	var rc int = 0

	if sliceOffset+int64(reqLen) <= me.meta.SliceSize {
		n = int64(reqLen)
	} else {
		n = sliceOffset + int64(reqLen) - me.meta.SliceSize
	}

	//the first part
	name := fmt.Sprintf("$slice$/%s/files/%d.dat", me.FileName, sliceNum)
	rc = me.io.GetBuffer(name, data[0:n], sliceOffset)
	if n == int64(reqLen) {
		return rc
	}

	//no more slice
	if sliceNum >= me.meta.SliceCount {
		return rc
	}

	//the second part falls into next slice
	sliceNum++
	n = int64(reqLen) - n
	name = fmt.Sprintf("$slice$/%s/files/%d.dat", me.FileName, sliceNum)
	rc = me.io.GetBuffer(name, data[n:], 0)
	if rc == int(n) {
		return reqLen
	} else if rc >= 0 && rc < int(n) {
		return reqLen - (int(n) - rc)
	} else {
		return rc
	}

	return 0
}

//append a block to slice file
func (me *SliceFile) Append(blocks []int64, data []byte) int {

	//appendLen := int64(len(blocks) * FILE_BLOCK_SIZE) + int64(len(data))
	//me.io.AppendBuffer(data, offset)
	return 0
}
