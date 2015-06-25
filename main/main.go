package main

import (
	//"fmt"
	"bytes"
	"encoding/json"
	log "github.com/Sirupsen/logrus"
	"github.com/topikachu/oss"
	"io"
	"os"
)

type Config struct {
	AccessKeyId, AccessKeySecret, Region, Bucket string
}

func main() {
	config := Config{}

	file, err := os.Open("config.json")
	if err != nil {
		panic(err)
	}
	decoder := json.NewDecoder(file)
	err = decoder.Decode(&config)
	if err != nil {
		panic(err)
	}

	log.SetLevel(log.InfoLevel)

	// Region list:
	// HangZhou         = "oss-cn-hangzhou"
	// QingDao          = "oss-cn-qingdao"
	// HangZhouInternal = "oss-cn-hangzhou-internal"
	// QingdaoInternal  = "oss-cn-qingdao-internal"
	// DefaultRegion    = "oss"
	api := oss.New(config.Region, config.AccessKeyId, config.AccessKeySecret, config.Bucket)
	fh, err := os.Open("/home/pikachu/go/src/github.com/topikachu/oss/oss_api.go")
	defer fh.Close()
	if err != nil {
		panic(err)
	}

	var buffer bytes.Buffer
	io.Copy(&buffer, fh)
	sendbytes := buffer.Bytes()
	err = api.PutObject("newossapi", sendbytes, "text/plain")
	if err != nil {
		panic(err)
	}
	log.Debugf("sent contents")

	receivebytes, err := api.GetObjectAsBytes("newossapi")
	if err != nil {
		panic(err)
	}
	log.Debugf("receive contents, size is %d", len(receivebytes))

	if bytes.Compare(sendbytes, receivebytes) != 0 {
		panic("the received content are not same as sent")
	}

	r, statusCode, err := api.GetObjectAsStream("newossapi", 0, 19)
	if err != nil {
		panic(err)
	}
	buffer.Reset()
	io.Copy(&buffer, r)
	r.Close()

	log.Debugf("get ranged content, size is %d, status code is %d\n", len(buffer.Bytes()), statusCode)

	r, statusCode, err = api.GetObjectAsStream("newossapi", 19, -1)
	if err != nil {
		panic(err)
	}
	buffer.Reset()
	io.Copy(&buffer, r)
	r.Close()

	log.Debugf("get ranged content, size is %d, status code is %d\n", len(buffer.Bytes()), statusCode)

	r, statusCode, err = api.GetObjectAsStream("newossapi", 0, -1)
	if err != nil {
		panic(err)
	}
	buffer.Reset()
	io.Copy(&buffer, r)
	r.Close()
	log.Debugf("get ranged content, size is %d, status code is %d\n", len(buffer.Bytes()), statusCode)

	r, statusCode, err = api.GetObjectAsStream("newossapi", -1, -1)
	if err != nil {
		panic(err)
	}
	buffer.Reset()
	io.Copy(&buffer, r)
	r.Close()
	log.Debugf("get ranged content, size is %d, status code is %d\n", len(buffer.Bytes()), statusCode)

	context, err := api.InitMultipartUpload("newmulti", "text/plain")
	if err != nil {
		panic(err)
	}
	log.Debugf("upload context %+v\n", context)
	err = api.UploadMultipart(context, sendbytes, 1)
	if err != nil {
		panic(err)
	}
	log.Debugf("upload context %+v\n", context)

	contexts, err := api.ListMultipartUploads()
	if err != nil {
		panic(err)
	}
	log.Debugf("upload context %+v\n", contexts)

	for _, context := range contexts {
		api.AbortMultipart(context)
	}

	context, err = api.InitMultipartUpload("newmulti", "text/plain")
	if err != nil {
		panic(err)
	}
	log.Infof("upload context %+v\n", context)
	err = api.UploadMultipart(context, sendbytes, 1)
	if err != nil {
		panic(err)
	}
	log.Infof("upload context %+v\n", context)

	err = api.CompleteMultipart(context)
	if err != nil {
		panic(err)
	}
	log.Infof("upload context %+v\n", context)

}
