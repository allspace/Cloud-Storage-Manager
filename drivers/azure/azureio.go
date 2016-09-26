package azureimpl
import (
    "fmt"
    "log"
    "encoding/base64"
    az "github.com/Azure/azure-sdk-for-go/storage"
)

type AzureIO struct {
    containerName string
    client *az.BlobStorageClient
}

func (me *AzureIO) ReadDir(path string)int{
    params := az.ListBlobsParameters{
        Prefix     : path,
        Delimiter  : "/",
    }
    rsp,err := me.client.ListBlobs(me.containerName, params)
    if err != nil {
        log.Println(err)
        return -5 //EIO
    }
    
    for _,file := range rsp.Blobs {
        log.Println(file.Name)
    }
    for _,folder := range rsp.BlobPrefixes {
        log.Println(folder)
    }

    return 0
}

func (me *AzureIO) PutBuffer(path string, data []byte)int {
    var err error
    //err := me.client.CreateBlockBlob(me.containerName, path)
    //if err != nil {
    //    log.Println(err)
    //    return -5//EIO
    //}
    blocks := make([]az.Block, 1)
    bid := base64.StdEncoding.EncodeToString([]byte("01"))
    blocks[0].ID=bid
    blocks[0].Status = az.BlockStatusUncommitted  
    err = me.client.PutBlock(me.containerName, path, bid, data)
    if err != nil {
        log.Println(err)
        return -5//EIO
    }
    err = me.client.PutBlockList(me.containerName, path, blocks)
    if err != nil {
        log.Println(err)
        return -5//EIO
    }
    return len(data)
}

func (me *AzureIO) GetBuffer(path string, data []byte, offset int64)int {
    byteRange := fmt.Sprintf("%d-%d", offset, offset + int64(len(data)))
    io,err := me.client.GetBlobRange(me.containerName, path, byteRange, nil)
    if err != nil {
        log.Println(err)
        return -5
    }
    n,err := io.Read(data)
    io.Close()
    return n
}

