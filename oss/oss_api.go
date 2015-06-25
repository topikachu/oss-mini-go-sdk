package oss

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
)

type OssApi struct {
	region, accessKeyId, accessKeySecret, bucket string
}

type part struct {
	PartNumber int
	ETag       string
}
type partSlice []part
type UploadContext struct {
	Key, UploadId string
	Parts         partSlice
}

func (s partSlice) Len() int           { return len(s) }
func (s partSlice) Less(i, j int) bool { return s[i].PartNumber < s[j].PartNumber }
func (s partSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func (context *UploadContext) addPart(partNumber int, etag string) {
	newpart := part{partNumber, etag}
	for i, part := range context.Parts {
		if partNumber == part.PartNumber {
			context.Parts[i] = newpart
			return
		}
	}
	context.Parts = append(context.Parts, newpart)
}

type request struct {
	method  string
	object  string
	params  url.Values
	headers http.Header
	baseurl string
	payload io.Reader
}

func New(region, accessKeyId, accessKeySecret, bucket string) OssApi {
	return OssApi{region, accessKeyId, accessKeySecret, bucket}
}

func (api *OssApi) PutObject(object string, contents []byte, contentType string) error {
	req := &request{
		method: "PUT",
		object: object,
		headers: map[string][]string{
			"Content-Length": {strconv.FormatInt(int64(len(contents)), 10)},
			"Content-Type":   {contentType},
		},
		payload: bytes.NewReader(contents),
	}
	return api.query(req, nil)
	return nil
}

type Header struct {
	http.Header
}

func (header *Header) GetContentLength() int64 {
	contentLengthString := header.Get("Content-Length")
	lenth, err := strconv.ParseInt(contentLengthString, 10, 64)
	if err != nil {
		return 0
	} else {
		return lenth
	}
}

func (api *OssApi) GetObjectMetadata(object string) (*Header, error) {
	req := &request{
		method: "HEAD",
		object: object,
	}
	hresp, err := api.rawQuery(req)
	if err != nil {
		return nil, err
	}
	defer hresp.Body.Close()
	return &Header{hresp.Header}, nil
}

func (api *OssApi) GetObjectAsStream(object string, start int64, end int64) (io.ReadCloser, int, error) {
	var headers = make(map[string][]string)
	if start >= 0 || end >= 0 {
		if start < 0 {
			start = 0
		}
		if end >= 0 {
			headers["Range"] = []string{fmt.Sprintf("bytes=%d-%d", start, end)}
		} else {
			headers["Range"] = []string{fmt.Sprintf("bytes=%d-", start)}
		}
	}
	req := &request{
		method:  "GET",
		object:  object,
		headers: headers,
	}
	hresp, err := api.rawQuery(req)
	if err != nil {
		return nil, -1, err
	}
	return hresp.Body, hresp.StatusCode, nil
}

func (api *OssApi) GetObjectAsBytes(object string) ([]byte, error) {
	reader, _, err := api.GetObjectAsStream(object, -1, -1)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	var buf bytes.Buffer
	_, err = io.Copy(&buf, reader)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (api *OssApi) InitMultipartUpload(object, contentType string) (*UploadContext, error) {
	req := &request{
		method: "POST",
		object: object,
		headers: map[string][]string{
			"Content-Type": {contentType},
		},
		params: map[string][]string{
			"uploads": {""},
		},
	}

	var resp struct {
		UploadId string `xml:"UploadId"`
	}
	err := api.query(req, &resp)
	if err != nil {
		return nil, err
	}
	return &UploadContext{
		Key:      object,
		UploadId: resp.UploadId,
	}, nil
}

func (api *OssApi) ListMultipartUploads() ([]*UploadContext, error) {
	req := &request{
		method: "GET",
		params: map[string][]string{
			"uploads": {""},
		},
	}

	var resp struct {
		Upload []struct {
			Key      string
			UploadId string
		}
	}

	err := api.query(req, &resp)
	if err != nil {
		return nil, err
	}
	var contexts []*UploadContext
	for _, upload := range resp.Upload {
		contexts = append(contexts, &UploadContext{
			Key:      upload.Key,
			UploadId: upload.UploadId,
		})
	}
	return contexts, nil
}

func (api *OssApi) FetchMultipartUploadParts(context *UploadContext) error {
	req := &request{
		method: "GET",
		object: context.Key,
		params: map[string][]string{
			"uploadId": {context.UploadId},
		},
	}

	var resp struct {
		Part []struct {
			PartNumber int
			ETag       string
		}
	}

	err := api.query(req, &resp)
	if err != nil {
		return err
	}

	for _, part := range resp.Part {
		context.addPart(part.PartNumber, part.ETag)
	}
	return nil
}

func (api *OssApi) UploadMultipart(context *UploadContext, contents []byte, partNumber int) error {
	req := &request{
		method: "PUT",
		object: context.Key,
		headers: map[string][]string{
			"Content-Length": {strconv.FormatInt(int64(len(contents)), 10)},
		},
		params: map[string][]string{
			"partNumber": {strconv.Itoa(partNumber)},
			"uploadId":   {context.UploadId},
		},
		payload: bytes.NewReader(contents),
	}
	hresp, err := api.rawQuery(req)
	if err != nil {
		return err
	}
	etag := hresp.Header.Get("ETag")
	context.addPart(partNumber, etag)
	return nil
}

func (api *OssApi) CompleteMultipart(context *UploadContext) error {

	var completeUpload struct {
		XMLName xml.Name  `xml:"CompleteMultipartUpload"`
		Parts   partSlice `xml:"Part"`
	}
	completeUpload.Parts = context.Parts
	sort.Sort(completeUpload.Parts)
	data, err := xml.Marshal(&completeUpload)
	if err != nil {
		return err
	}
	req := &request{
		method: "POST",
		object: context.Key,
		params: map[string][]string{
			"uploadId": {context.UploadId},
		},
		payload: bytes.NewReader(data),
	}
	return api.query(req, nil)
}

func (api *OssApi) AbortMultipart(context *UploadContext) error {
	req := &request{
		method: "DELETE",
		object: context.Key,
		params: map[string][]string{
			"uploadId": {context.UploadId},
		},
	}
	return api.query(req, nil)
}
