package fsvc

import (
	"log"
	"time"

	"context"
	"strings"

	"github.com/keybase/kbfs/dokan"
	"github.com/keybase/kbfs/dokan/winacl"

	"brightlib.com/common"
)

func FileSystemMainLoop(fs fscommon.FileSystemImpl, mnt string) {
	csfs := &CSFileSystem{fsbk: fs}

	mp, err := dokan.Mount(&dokan.Config{FileSystem: csfs, Path: `Q:`})
	if err != nil {
		log.Fatal("Mount failed:", err)
	}
	err = mp.BlockTillDone()
	if err != nil {
		log.Println("Filesystem exit:", err)
	}
}

type CSFileSystem struct {
	fsbk fscommon.FileSystemImpl
}

type CSFile struct {
	fsbk fscommon.FileSystemImpl
	fibk *fscommon.FileObject
}

func (me *CSFileSystem) WithContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return ctx, nil
}

func (me *CSFileSystem) CreateFile(ctx context.Context, fi *dokan.FileInfo, data *dokan.CreateData) (file dokan.File, isDirectory bool, err error) {
	log.Println("CreateFile on path: ", fi.Path())

	path := strings.Replace(fi.Path(), "\\", "/", -1)
	isDir := ((data.CreateOptions & dokan.FileDirectoryFile) != 0)
	if len(path) == 0 {
		log.Printf("%s, %s: Is Dir = %d", fi.Path(), path, isDir)
		return nil, isDir, dokan.ErrObjectPathNotFound
	}

	//try to check target first
	di, ok := me.fsbk.GetAttr(path)
	if ok < 0 {
		if ok == fscommon.ENOENT {
			if data.CreateDisposition != dokan.FileCreate {
				return nil, isDir, dokan.ErrObjectPathNotFound
			}
		} else {
			return nil, isDir, dokan.ErrAccessDenied
		}
	}
	if di.Type == fscommon.S_IFDIR {
		isDir = true
	}

	//for directory operations
	if isDir == true {
		log.Printf("CreateFile on direcotry: %s", path)
		if data.CreateDisposition == dokan.FileCreate {
			ok := me.fsbk.Mkdir(path, 0)
			if ok != 0 {
				return nil, true, dokan.ErrAccessDenied
			} else {
				return &CSFile{fsbk: me.fsbk}, true, nil
			}
		}
		return &CSFile{fsbk: me.fsbk}, isDir, nil
	} else { //for file operations
		log.Printf("CreateFile on file: %s", path)
		var flags uint32
		if data.CreateDisposition == dokan.FileCreate {
			flags |= fscommon.O_CREAT
		}
		fh, ok := me.fsbk.Open(path, flags)
		if ok == 0 {
			return &CSFile{fsbk: me.fsbk, fibk: fh}, false, nil
		} else {
			return nil, false, dokan.ErrAccessDenied
		}
	}
}

func (me *CSFileSystem) GetDiskFreeSpace(ctx context.Context) (dokan.FreeSpace, error) {
	return dokan.FreeSpace{
		FreeBytesAvailable:     1024 * 1024 * 1024 * 1024,
		TotalNumberOfBytes:     1024 * 1024 * 1024 * 1024,
		TotalNumberOfFreeBytes: 1024 * 1024 * 1024 * 1024,
	}, nil
}

func (me *CSFileSystem) GetVolumeInformation(ctx context.Context) (dokan.VolumeInformation, error) {
	return dokan.VolumeInformation{
		VolumeName:             "CSMgr",
		VolumeSerialNumber:     0,
		MaximumComponentLength: 255,
		FileSystemFlags:        dokan.FileCaseSensitiveSearch,
		FileSystemName:         "CSMgr",
	}, nil
}

func (me *CSFileSystem) MoveFile(ctx context.Context, source *dokan.FileInfo, targetPath string, replaceExisting bool) error {
	return dokan.ErrNotSupported
}

///////////////////////////////////////////////////////////////////////////////

func (me *CSFile) FindFiles(ctx context.Context, fi *dokan.FileInfo, pattern string, fillStatCallback func(*dokan.NamedStat) error) error {
	log.Println("FindFiles on path： ", fi.Path())

	path := strings.Replace(fi.Path(), "\\", "/", -1)

	dis, _ := me.fsbk.ReadDir(path)
	for _, di := range dis {
		var fa dokan.FileAttribute
		if di.Type == fscommon.S_IFREG {
			fa |= dokan.FileAttributeNormal
		} else {
			fa |= dokan.FileAttributeDirectory
		}
		st := &dokan.NamedStat{
			Name: di.Name,
			Stat: dokan.Stat{
				FileAttributes: fa,
				FileSize:       int64(di.Size),
				Creation:       di.Mtime,
				LastWrite:      di.Mtime,
				LastAccess:     time.Now(),
			},
		}
		fillStatCallback(st)
	}
	return nil
}

func (me *CSFile) GetFileInformation(ctx context.Context, fi *dokan.FileInfo) (*dokan.Stat, error) {
	log.Println("GetFileInformation on path： ", fi.Path())

	path := strings.Replace(fi.Path(), "\\", "/", -1)

	di, ok := me.fsbk.GetAttr(path)
	if ok != 0 {
		return nil, dokan.ErrObjectPathNotFound
	}

	var fa dokan.FileAttribute
	if di.Type == fscommon.S_IFREG {
		fa |= dokan.FileAttributeNormal
	} else {
		fa |= dokan.FileAttributeDirectory
	}

	return &dokan.Stat{
		FileAttributes:     fa,
		Creation:           di.Mtime,
		LastAccess:         time.Now(),
		LastWrite:          di.Mtime,
		VolumeSerialNumber: 0,
		FileSize:           int64(di.Size),
		FileIndex:          1,
	}, nil
}

func (me *CSFile) Cleanup(ctx context.Context, fi *dokan.FileInfo) {

}

func (me *CSFile) CloseFile(ctx context.Context, fi *dokan.FileInfo) {
	if me.fibk != nil {
		me.fibk.Release()
	}
}

func (me *CSFile) CanDeleteFile(ctx context.Context, fi *dokan.FileInfo) error {
	return dokan.ErrAccessDenied
}
func (me *CSFile) CanDeleteDirectory(ctx context.Context, fi *dokan.FileInfo) error {
	return dokan.ErrAccessDenied
}
func (me *CSFile) SetEndOfFile(ctx context.Context, fi *dokan.FileInfo, length int64) error {
	log.Println("emptyFile.SetEndOfFile")
	return nil
}
func (me *CSFile) SetAllocationSize(ctx context.Context, fi *dokan.FileInfo, length int64) error {
	log.Println("emptyFile.SetAllocationSize")
	return nil
}
func (me *CSFile) MoveFile(ctx context.Context, source *dokan.FileInfo, targetPath string, replaceExisting bool) error {
	log.Println("emptyFS.MoveFile")
	return nil
}
func (me *CSFile) ReadFile(ctx context.Context, fi *dokan.FileInfo, bs []byte, offset int64) (int, error) {
	n := me.fibk.Read(bs, offset)
	if n < 0 {
		return n, nil
	} else {
		return 0, dokan.ErrAccessDenied
	}
}
func (me *CSFile) WriteFile(ctx context.Context, fi *dokan.FileInfo, bs []byte, offset int64) (int, error) {
	return len(bs), nil
}
func (me *CSFile) FlushFileBuffers(ctx context.Context, fi *dokan.FileInfo) error {
	log.Println("emptyFS.FlushFileBuffers")
	return nil
}

func (me *CSFile) SetFileTime(context.Context, *dokan.FileInfo, time.Time, time.Time, time.Time) error {
	log.Println("emptyFile.SetFileTime")
	return nil
}
func (me *CSFile) SetFileAttributes(ctx context.Context, fi *dokan.FileInfo, fileAttributes dokan.FileAttribute) error {
	log.Println("emptyFile.SetFileAttributes")
	return nil
}

func (me *CSFile) LockFile(ctx context.Context, fi *dokan.FileInfo, offset int64, length int64) error {
	log.Println("emptyFile.LockFile")
	return nil
}
func (me *CSFile) UnlockFile(ctx context.Context, fi *dokan.FileInfo, offset int64, length int64) error {
	log.Println("emptyFile.UnlockFile")
	return nil
}

func (me *CSFile) GetFileSecurity(ctx context.Context, fi *dokan.FileInfo, si winacl.SecurityInformation, sd *winacl.SecurityDescriptor) error {
	log.Println("emptyFS.GetFileSecurity")
	return nil
}
func (me *CSFile) SetFileSecurity(ctx context.Context, fi *dokan.FileInfo, si winacl.SecurityInformation, sd *winacl.SecurityDescriptor) error {
	log.Println("emptyFS.SetFileSecurity")
	return nil
}
