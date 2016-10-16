package main

import (
	//"fmt"
	"flag"
	"log"

	"github.com/allspace/csmgr/fsvc"
	cfg "github.com/allspace/csmgr/util"
)

func main() {
	flag.String("vendor_type", "S3", "file system type")
	flag.Parse()
	if len(flag.Args()) < 1 {
		//log.Fatal("Usage:\n  hello MOUNTPOINT")
	}

	log.SetFlags(log.LstdFlags | log.Lshortfile)

	cfg.Default.Init("")
	cfg.Default.PrintAll()

	fs, _ := NewFileSystem(cfg.Default.GetStringEx("VENDOR_TYPE", ""))
	if fs == nil {
		log.Println("Failed to create file system instance.")
		return
	}
	//fsvc.FileSystemMainLoop(fs, flag.Arg(0))
	fsvc.Http_MainLoop(fs)
}
