package oss

import (
	"bytes"
	//"crypto/rand"
	//"encoding/hex"
	"encoding/json"
	log "github.com/Sirupsen/logrus"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"
)

type Config struct {
	AccessKeyId, AccessKeySecret, Region, Bucket, LogLevel string
}

var config Config

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randSeq(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

var contents = make([]byte, 1*1024*1024)

var api *OssApi

func getNewFileName() string {
	rand.Seed(time.Now().UTC().UnixNano())
	return randSeq(16)
}

func setLogLevelFromConfig() {
	logLevel, err := log.ParseLevel(strings.ToLower(config.LogLevel))
	if err != nil {
		log.SetLevel(log.InfoLevel)
	} else {
		log.SetLevel(logLevel)
	}
}

var randomFolder = getNewFileName() + "/"

var objectFile1 = "test/" + randomFolder + "objectFile1"
var multipartFile1 = "test/" + randomFolder + "multiobject1"
var multipartFile2 = "test/" + randomFolder + "multiobject2"
var multipartFile3 = "test/" + randomFolder + "multiobject3"
var multipartFile4 = "test/" + randomFolder + "multiobject4"
var multipartFile5 = "test/" + randomFolder + "multiobject5"
var multipartFile6 = "test/" + randomFolder + "multiobject6"
var objectFile2 = "test/" + randomFolder + "objectFile2"

var folderNameForList = "test/" + randomFolder + "listfolder/"
var fileNameForList = []string{
	folderNameForList + "file1",
	folderNameForList + "file2",
	folderNameForList + "subfolder1/file3",
	folderNameForList + "subfolder1/file4",
	folderNameForList + "subfolder2/file5",
	folderNameForList + "subfolder2/file6",
	folderNameForList + "subfolder3/subfolder3_1/file7",
	folderNameForList + "subfolder3/subfolder3_2/file8",
	folderNameForList + "subfolder4/",
}

func TestMain(m *testing.M) {
	rand.Seed(time.Now().UTC().UnixNano())
	file, err := os.Open("config.json")
	if err != nil {
		panic(err)
	}
	decoder := json.NewDecoder(file)
	err = decoder.Decode(&config)
	if err != nil {
		panic(err)
	}

	if config.AccessKeyId == "" || config.AccessKeySecret == "" || config.Region == "" || config.Bucket == "" {
		panic("config.json is not valid")
	}
	setLogLevelFromConfig()

	contents = bytes.NewBufferString(randSeq(1024 * 1024)).Bytes()
	api = New(config.Region, config.AccessKeyId, config.AccessKeySecret, config.Bucket)
	if api == nil {
		panic("Unable new oss")
	}
	err = api.PutObject(objectFile1, contents, "text/plain")
	log.Infof("put object %s", objectFile1)
	if err != nil {
		panic("Unable put object:" + err.Error())
	}
	ret := m.Run()
	contextList, _, _ := api.ListMultipartUploads("test/", nil, -1)
	for _, context := range contextList {
		api.AbortMultipart(context)
	}
	os.Exit(ret)
}

func TestGetObjectMetadata(t *testing.T) {
	header, err := api.GetObjectMetadata(objectFile1)

	if header == nil || err != nil {
		t.Errorf("Unable get object metadata", err)
	}
	if header.GetContentLength() <= 0 {
		t.Errorf("No content length")
	}

	if header.Get("Date") == "" {
		t.Errorf("No Date")
	}

}

func TestGetObject(t *testing.T) {
	received, _, err := api.GetObject(objectFile1)
	if err != nil {
		t.Errorf("Unable get object: %s", objectFile1, err)
	}
	if len(received) == 0 {
		t.Errorf("no content received")

	}

	if bytes.Compare(contents, received) != 0 {
		t.Errorf("the received content are not same as sent")
	}

}

func TestGetObjectRangeStartEnd(t *testing.T) {

	received, statusCode, err := api.GetObjectRange(objectFile1, 1, 20)
	if err != nil {
		t.Errorf("Unable get object: %s", objectFile1, err)
	}
	if statusCode != 206 {
		t.Errorf("wrong status code expected %d, actual %d", 206, statusCode)

	}

	if len(received) != 20 {
		t.Errorf("wrong content size expected %d, actual %d", 20, len(received))

	}

	if bytes.Compare(contents[1:21], received) != 0 {
		t.Errorf("the received content are not same as sent")
	}

}

func TestGetObjectRangeOnlyStart(t *testing.T) {

	received, statusCode, err := api.GetObjectRange(objectFile1, 1, -1)
	if err != nil {
		t.Errorf("Unable get object: %s", objectFile1, err)
	}
	if statusCode != 206 {
		t.Errorf("wrong status code expected %d, actual %d", 206, statusCode)

	}

	if len(received) != len(contents)-1 {
		t.Errorf("wrong content size expected %d, actual %d", len(contents)-1, len(received))

	}

	if bytes.Compare(contents[1:], received) != 0 {
		t.Errorf("the received content are not same as sent")
	}

}

func TestGetObjectRangeNoStartNoEnd(t *testing.T) {

	received, statusCode, err := api.GetObjectRange(objectFile1, -1, -1)
	if err != nil {
		t.Errorf("Unable get object: %s", objectFile1, err)
	}
	if statusCode != 200 {
		t.Errorf("wrong status code expected %d, actual %d", 200, statusCode)

	}

	if len(received) != len(contents) {
		t.Errorf("wrong content size expected %d, actual %d", len(contents), len(received))

	}

	if bytes.Compare(contents, received) != 0 {
		t.Errorf("the received content are not same as sent")
	}

}

func TestMultiUpload(t *testing.T) {

	context, err := api.InitMultipartUpload(multipartFile1, "text/plain")
	if err != nil {
		t.Errorf("cant init multi upload", err)
	}
	err = api.UploadMultipart(context, contents, 1)
	if err != nil {
		t.Errorf("cant upload multi part", err)
	}
	err = api.CompleteMultipart(context)
	if err != nil {
		t.Errorf("cant complete multi part", err)
	}
	received, _, err := api.GetObject(multipartFile1)
	if bytes.Compare(contents, received) != 0 {
		t.Errorf("the received content are not same as sent")
	}
}

func TestListMultiUpload(t *testing.T) {

	_, err := api.InitMultipartUpload(multipartFile2, "text/plain")
	if err != nil {
		t.Errorf("cant init multi upload", err)
	}
	_, err = api.InitMultipartUpload(multipartFile2, "text/plain")
	if err != nil {
		t.Errorf("cant init multi upload", err)
	}
	_, err = api.InitMultipartUpload(multipartFile2, "text/plain")
	if err != nil {
		t.Errorf("cant init multi upload", err)
	}
	contextList, _, err := api.ListMultipartUploads(multipartFile2, nil, -1)
	if err != nil {
		t.Errorf("cant list multi upload", err)
	}
	if len(contextList) != 3 {
		t.Errorf("wrong upload context size expected %d, actual %d", 3, len(contextList))
	}
	for _, context := range contextList {
		api.AbortMultipart(context)
	}

}

func TestFetchMultiUpload(t *testing.T) {
	context, err := api.InitMultipartUpload(multipartFile3, "text/plain")
	if err != nil {
		t.Errorf("cant init multi upload", err)
	}
	err = api.UploadMultipart(context, contents, 1)
	if err != nil {
		t.Errorf("cant upload multi part", err)
	}
	err = api.UploadMultipart(context, contents, 2)
	if err != nil {
		t.Errorf("cant upload multi part", err)
	}
	err = api.FetchMultipartUploadParts(context)
	if err != nil {
		t.Errorf("cant fetch multi part", err)
	}
	if len(context.Parts) != 2 {
		t.Errorf("wrong upload parts size expected %d, acutal%d", 2, len(context.Parts))
	}
	api.AbortMultipart(context)
}

func TestUploadCopyMultipart(t *testing.T) {
	context, err := api.InitMultipartUpload(multipartFile4, "text/plain")
	if err != nil {
		t.Errorf("cant init multi upload", err)
	}

	remaining, err := api.UploadCopyMultipart(context, "", objectFile1, -1, -1, 1)
	if err != nil {
		t.Errorf("cant uploadcopy multi", err)
	}
	if remaining != 0 {
		t.Errorf("Error, there's remaining &d bytes", remaining)
	}

	err = api.CompleteMultipart(context)
	if err != nil {
		t.Errorf("cant complete multi part", err)
	}

	received, _, err := api.GetObject(multipartFile4)
	if bytes.Compare(contents, received) != 0 {
		t.Errorf("the received content are not same as sent")
	}

}

func TestUploadCopyMultipartStartEnd(t *testing.T) {

	context, err := api.InitMultipartUpload(multipartFile5, "text/plain")
	if err != nil {
		t.Errorf("cant init multi upload", err)
	}
	remaining, err := api.UploadCopyMultipart(context, "", objectFile1, 10, 20, 1)
	if err != nil {
		t.Errorf("cant uploadcopy multi", err)
	}

	if remaining == 0 {
		t.Errorf("Error, no remaining bytes %d", remaining)
	}

	err = api.CompleteMultipart(context)
	if err != nil {
		t.Errorf("cant complete multi part", err)
	}

	received, _, err := api.GetObject(multipartFile5)
	if bytes.Compare(contents[10:21], received) != 0 {
		t.Errorf("the received content are not same as sent")
	}

}

func TestUploadCopyMultipartSingleStart(t *testing.T) {

	context, err := api.InitMultipartUpload(multipartFile6, "text/plain")
	if err != nil {
		t.Errorf("cant init multi upload", err)
	}
	remaining, err := api.UploadCopyMultipart(context, "", objectFile1, 10, -1, 1)
	if err != nil {
		t.Errorf("cant uploadcopy multi", err)
	}
	if remaining != 0 {
		t.Errorf("Error, there's remaining %d bytes", remaining)
	}
	err = api.CompleteMultipart(context)
	if err != nil {
		t.Errorf("cant complete multi part", err)
	}

	received, _, err := api.GetObject(multipartFile6)
	if bytes.Compare(contents[10:], received) != 0 {
		t.Errorf("the received content are not same as sent")
	}

}

func TestCopy(t *testing.T) {

	err := api.Copy("", objectFile1, objectFile2, "text/plain", 0)
	if err != nil {
		t.Errorf("cant init multi upload", err)
	}

	received, _, err := api.GetObject(objectFile2)
	if bytes.Compare(contents, received) != 0 {
		t.Errorf("the received content are not same as sent")
	}

}

func TestListFiles(t *testing.T) {
	for _, fileName := range fileNameForList {
		api.PutObject(fileName, contents, "text/plain")
	}

	fileNames, folderNames, _, err := api.ListFiles(folderNameForList, "/", "", -1)
	if err != nil {
		t.Errorf("cant ListFiles", err)
	}

	if len(fileNames) != 2 {
		t.Errorf("wrong files return size, expected %d, actual %d", 2, len(fileNames))
	}

	if len(folderNames) != 4 {
		t.Errorf("wrong folders return size, expected %d, actual %d", 4, len(folderNames))
	}

	fileNames, folderNames, _, err = api.ListFiles(folderNameForList, "", "", -1)
	if err != nil {
		t.Errorf("cant ListFiles", err)
	}

	if len(fileNames) != 9 {
		t.Errorf("wrong files return size, expected %d, actual %d", 9, len(fileNames))
	}

	if len(folderNames) != 0 {
		t.Errorf("wrong folders return size, expected %d, actual %d", 0, len(folderNames))
	}

}

func TestDelete(t *testing.T) {
	err := api.Delete(folderNameForList)
	if err != nil {
		t.Errorf("delete failed", err)
	}
}

// func TestPut(t *testing.T) {
// 	bucket := New(DefaultRegion, accessId, accessKey).Bucket(testBucket)
// 	data := []byte("helloworld")
// 	err := bucket.Put("readme", data, "text/plain", Private)
// 	if err != nil {
// 		t.Errorf("Unable put object:", err)
// 	}
// }

// func TestGet(t *testing.T) {
// 	bucket := New(DefaultRegion, accessId, accessKey).Bucket(testBucket)
// 	data, err := bucket.Get("readme")
// 	if err != nil {
// 		t.Errorf("Unable get object:", err)
// 		return
// 	}
// 	log.Println(string(data))
// }

// func TestDel(t *testing.T) {
// 	bucket := New(DefaultRegion, accessId, accessKey).Bucket(testBucket)
// 	err := bucket.Del("readme")
// 	if err != nil {
// 		t.Errorf("Unable del object:", err)
// 	}
// }

// func TestDelBucket(t *testing.T) {
// 	bucket := New(DefaultRegion, accessId, accessKey).Bucket(testBucket)
// 	err := bucket.DelBucket()
// 	if err != nil {
// 		t.Errorf("Unable del bucket:", err)
// 	}
// }

// func TestURL(t *testing.T) {
// 	bucket := New(DefaultRegion, accessId, accessKey).Bucket(testBucket)
// 	url := bucket.URL("readme")
// 	if url != "http://oss.aliyuncs.com/pinidea-test/readme" {
// 		t.Errorf("Unable get correct url:", url)
// 	}
// }

// func TestPutBuceketWithRegion(t *testing.T) {
// 	bucket := New(QingDao, accessId, accessKey).Bucket("pinidea-test111")
// 	err := bucket.PutBucket(PublicRead)
// 	if err != nil {
// 		t.Errorf("Unable put bucket:", err)
// 	}
// }

// func TestPutWithRegion(t *testing.T) {
// 	bucket := New(QingDao, accessId, accessKey).Bucket("pinidea-test111")
// 	data := []byte("helloworld")
// 	err := bucket.Put("readme", data, "text/plain", Private)
// 	if err != nil {
// 		t.Errorf("Unable put object:", err)
// 	}
// }

// func TestDelWithRegion(t *testing.T) {
// 	bucket := New(QingDao, accessId, accessKey).Bucket("pinidea-test111")
// 	if err := bucket.Del("readme"); err != nil {
// 		t.Errorf("Unable del with region", err)
// 	}
// }

// func TestDelBucketWithRegion(t *testing.T) {
// 	bucket := New(QingDao, accessId, accessKey).Bucket("pinidea-test111")
// 	err := bucket.DelBucket()
// 	if err != nil {
// 		t.Errorf("Unable del bucket with region", err)
// 	}
// }