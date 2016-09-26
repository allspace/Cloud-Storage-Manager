package azureimpl

import (
    "log"
    az "github.com/Azure/azure-sdk-for-go/storage" 
)

type AzureClientImpl struct {
    client az.BlobStorageClient   
}

func NewClient()*AzureClientImpl {
    return &AzureClientImpl{}
}

func (me *AzureClientImpl) Connect(endPoint string, keyId string, keyData string)(int) {

    clt,err := az.NewClient(keyId, keyData, az.DefaultBaseURL, az.DefaultAPIVersion, true)
    if err != nil {
        log.Println(err)
        return -1
    }
    me.client = clt.GetBlobService()
    return 0
}


func (me *AzureClientImpl) Mount(name string)*AzureIO{
    return &AzureIO{ containerName : name, client : &me.client }
}

