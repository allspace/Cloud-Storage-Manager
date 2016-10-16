package fsvc

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"time"

	"golang.org/x/net/webdav"

	"github.com/allspace/csmgr/common"
	csu "github.com/allspace/csmgr/util"
)

func Http_MainLoop(fs fscommon.FileSystemImpl) {
	//	myHandler := &fsvc_Handler{fs: fs}

	//	s := &http.Server{
	//		Addr:           ":8080",
	//		Handler:        myHandler,
	//		ReadTimeout:    10 * time.Second,
	//		WriteTimeout:   10 * time.Second,
	//		MaxHeaderBytes: 1 << 20,
	//	}
	//	s.ListenAndServe()

	h := &webdav.Handler{
		FileSystem: webDavFS{fs: fs},
		LockSystem: webDavLS{},
		Logger: func(r *http.Request, err error) {
			litmus := r.Header.Get("X-Litmus")
			if len(litmus) > 19 {
				litmus = litmus[:16] + "..."
			}

			switch r.Method {
			case "COPY", "MOVE":
				dst := ""
				if u, err := url.Parse(r.Header.Get("Destination")); err == nil {
					dst = u.Path
				}
				o := r.Header.Get("Overwrite")
				log.Printf("%-20s%-10s%-30s%-30so=%-2s%v", litmus, r.Method, r.URL.Path, dst, o, err)
			default:
				log.Printf("%-20s%-10s%-30s%v", litmus, r.Method, r.URL.Path, err)
			}
		},
	}

	http.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.ServeHTTP(w, r)
	}))

	addr := fmt.Sprintf(":%d", 8080)
	log.Printf("Serving %v", addr)
	http.ListenAndServe(addr, nil)

}

type fsvc_Handler struct {
	fs fscommon.FileSystemImpl
}

func (me *fsvc_Handler) ServeHTTP(rsp http.ResponseWriter, req *http.Request) {
	log.Println("Request for ", req.URL)
	path := req.URL.Path
	if path[len(path)-1] == '/' {
		me.listDir(rsp, path)
	}

	//download file

}

func (me *fsvc_Handler) listDir(rsp http.ResponseWriter, path string) {
	html := "<html><table>"
	dis, ok := me.fs.ReadDir(path)
	if ok < 0 {
		rsp.WriteHeader(501)
		return
	}

	var name, link string
	for _, di := range dis {
		if di.IsDir() == false {
			name = di.Name()
			link = path + di.Name()
		} else {
			name = di.Name() + "/"
			link = path + di.Name() + "/"
		}
		html += "<tr>"
		html += "<td><a href='" + link + "'>" + name + "</a></td>"
		html += "</tr>"
	}
	html += "</table>"

	rsp.Write([]byte(html))
}

func (me *fsvc_Handler) downloadFile(rsp http.ResponseWriter, path string) {
	fo, _ := me.fs.Open(path, 0)
	buff := make([]byte, 1024*1024)
	offset := int64(0)
	n := 0

	for true {
		n = fo.Read(buff, offset)
		rsp.Write(buff)
		offset += int64(n)
		if n < len(buff) {
			break
		}
	}

	fo.Release()
}

type webDavFS struct {
	fs fscommon.FileSystemImpl
}

type webDavFile struct {
	fileName string
	fs       fscommon.FileSystemImpl
	fo       *fscommon.FileObject
	davFs    *webDavFS

	filePos int64
}

func (me webDavFS) Mkdir(name string, perm os.FileMode) error {
	return csu.ErrorCode(me.fs.Mkdir(name, 0))
}

func (me webDavFS) OpenFile(path string, flag int, perm os.FileMode) (webdav.File, error) {
	di, ok := me.fs.GetAttr(path)
	if ok != 0 {
		if (flag & os.O_CREATE) == 0 {
			return nil, csu.ErrorCode(ok)
		}
	}

	var fo *fscommon.FileObject
	if di == nil || di.IsDir() == false {
		fo, ok = me.fs.Open(path, uint32(flag))
		if ok != 0 {
			return nil, csu.ErrorCode(ok)
		}
	}

	return &webDavFile{
		fileName: path,
		fs:       me.fs,
		fo:       fo,
		davFs:    &me,
	}, nil
}

func (me webDavFS) RemoveAll(name string) error {
	return csu.ErrorCode(me.fs.Unlink(name))
}

func (me webDavFS) Rename(oldName, newName string) error {
	return csu.ErrorMsg("Not support")
}

func (me webDavFS) Stat(name string) (os.FileInfo, error) {
	log.Printf("Stat is called for %s", name)
	di, ok := me.fs.GetAttr(name)
	if ok < 0 {
		log.Printf("GetAttr failed with %d", ok)
		return nil, csu.ErrorCode(ok)
	}
	log.Printf("Stat return file %s length: %d", di.Name(), di.Size())
	return di, nil
}

func (me *webDavFile) Readdir(count int) ([]os.FileInfo, error) {
	log.Printf("ReadDir is called for %s", me.fileName)
	dis, ok := me.fs.ReadDir(me.fileName)
	if ok < 0 {
		log.Printf("Failed to call ReadDir: %d.", ok)
		return nil, csu.ErrorCode(ok)
	}
	return dis, nil
}

func (me *webDavFile) Stat() (os.FileInfo, error) {
	return me.davFs.Stat(me.fileName)
}

func (me *webDavFile) Close() error {
	log.Printf("Close is called for file %s", me.fileName)
	if me.fo != nil {
		me.fo.Release()
	}
	return nil
}

func (me *webDavFile) Read(data []byte) (int, error) {
	log.Printf("Read file %s at offset %d length %d", me.fileName, me.filePos, len(data))
	n := me.fo.Read(data, me.filePos)
	if n < 0 {
		return 0, csu.ErrorCode(n)
	}
	me.filePos += int64(n)
	return n, nil
}

func (me *webDavFile) Seek(offset int64, whence int) (int64, error) {
	log.Printf("Seek is called for file %s, offset %d, loc %d", me.fileName, offset, whence)
	switch whence {
	case io.SeekStart:
		me.filePos = offset
	case io.SeekCurrent:
		me.filePos += offset
	case io.SeekEnd:
		me.filePos = me.fo.GetLength() + offset

	}

	return me.filePos, nil
}

func (me *webDavFile) Write(data []byte) (int, error) {
	n := me.fo.Write(data, me.filePos)
	if n < 0 {
		return 0, csu.ErrorCode(n)
	}
	me.filePos += int64(n)
	return n, nil
}

type webDavLS struct{}

func (me webDavLS) Confirm(now time.Time, name0, name1 string, conditions ...webdav.Condition) (release func(), err error) {
	log.Printf("Confirm: %s", name0)
	release = me.Release
	err = nil
	return
}

func (me webDavLS) Create(now time.Time, details webdav.LockDetails) (token string, err error) {
	log.Printf("Create: %s", details.Root)
	return details.Root, nil
}

func (me webDavLS) Refresh(now time.Time, token string, duration time.Duration) (webdav.LockDetails, error) {
	log.Printf("Refresh: %s", token)
	return webdav.LockDetails{}, nil
}

func (me webDavLS) Unlock(now time.Time, token string) error {
	log.Printf("Unlock: %s", token)
	return nil
}

func (me webDavLS) Release() {

}
