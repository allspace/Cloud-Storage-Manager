// +build test

package main

import (
	"flag"
	"log"
	"os"
	//"bufio"
)

func main() {

	var fileName = flag.String("file", "", "target file path")

	flag.Parse()
	var buf = make([]byte, 64*1024)

	file, err := os.Create(*fileName)
	if err != nil {
		log.Fatal(err)
	}
	//reader := bufio.NewReader(os.Stdin)
	for i := 0; i < 10240; i++ {
		file.Write(buf)
		log.Println("write 64KB, press enter key to continue...")
		//reader.ReadString('\n')
	}
	file.Close()
}
