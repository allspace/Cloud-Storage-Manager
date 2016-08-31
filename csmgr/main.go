package main

import (
	"fmt"
	"os"
	"os/signal"
	"flag"
	"log"
	
	//"brightlib.com/drivers/s3"
	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
	//"brightlib.com/common"
)


func main() {

	flag.Parse()
	if len(flag.Args()) < 1 {
		log.Fatal("Usage:\n  hello MOUNTPOINT")
	}

	fs, _ := NewFileSystem("s3")
	
	nfs := pathfs.NewPathNodeFs(
				&HelloFs{FileSystem: pathfs.NewDefaultFileSystem(), FileSystemImpl: fs}, 
				&pathfs.PathNodeFsOptions{Debug:true})
				
	server, _, err := nodefs.MountRoot(
				flag.Arg(0), 
				nfs.Root(), 
				&nodefs.Options{Debug:true})
				
	if err != nil {
		log.Fatalf("Mount fail: %v\n", err)
	}
	
	//register signal handler
	go handleSignal(server)
	
	server.Serve()
	
}

func handleSignal(ms *fuse.Server){
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	// Block until a signal is received.
	s := <-c
	fmt.Println("Got signal:", s)
	
	//umount the mount point
	ms.Unmount()
}