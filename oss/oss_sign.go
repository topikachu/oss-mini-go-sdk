package oss

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	log "github.com/Sirupsen/logrus"
	"sort"
	"strings"
)

var b64 = base64.StdEncoding

// ----------------------------------------------------------------------------
// ali oss signing
// http://docs.aliyun.com/?spm=5176.383663.9.2.ZTrRme#/pub/oss/api-reference/access-control&signature-header
// however, the document is not cleare enough
// reference from the oss java sdk
// http://docs.aliyun.com/?spm=5176.383663.9.5.ZTrRme#/pub/oss/sdk/sdk-download&java

var ossSubResourceList = map[string]bool{
	"acl":                          true,
	"uploads":                      true,
	"location":                     true,
	"cors":                         true,
	"logging":                      true,
	"website":                      true,
	"referer":                      true,
	"lifecycle":                    true,
	"delete":                       true,
	"uploadId":                     true,
	"partNumber":                   true,
	"security-token":               true,
	"response-cache-control":       true,
	"response-content-disposition": true,
	"response-content-encoding":    true,
	"response-content-language":    true,
	"response-content-type":        true,
	"response-expires":             true,
}

func (api *OssApi) sign(method, object string, params, headers map[string][]string) {
	var md5, ctype, date, xoss string
	var xossHeaders, ossSubResources []string
	for k, v := range headers {
		k = strings.ToLower(k)
		switch k {
		case "content-md5":
			md5 = v[0]
		case "content-type":
			ctype = v[0]
		case "date":
			date = v[0]
		default:
			if strings.HasPrefix(k, "x-oss-") {
				xossHeaders = append(xossHeaders, k+":"+strings.Join(v, ","))
			}
		}
	}
	if len(xossHeaders) > 0 {
		sort.StringSlice(xossHeaders).Sort()
		xoss = strings.Join(xossHeaders, "\n") + "\n"
	}

	for k, v := range params {
		if ossSubResourceList[k] {
			for _, vi := range v {
				if vi == "" {
					ossSubResources = append(ossSubResources, k)
				} else {
					// "When signing you do not encode these values."
					ossSubResources = append(ossSubResources, k+"="+vi)
				}
			}
		}
	}
	canonicalPath := "/" + api.bucket + "/" + object
	if len(ossSubResources) > 0 {
		sort.StringSlice(ossSubResources).Sort()
		canonicalPath = canonicalPath + "?" + strings.Join(ossSubResources, "&")
	}

	payload := method + "\n" + md5 + "\n" + ctype + "\n" + date + "\n" + xoss + canonicalPath
	hash := hmac.New(sha1.New, []byte(api.accessKeySecret))
	hash.Write([]byte(payload))
	signature := make([]byte, b64.EncodedLen(hash.Size()))
	b64.Encode(signature, hash.Sum(nil))
	headers["Authorization"] = []string{"OSS " + api.accessKeyId + ":" + string(signature)}

	log.Debugf("Signature payload: %q", payload)
	log.Debugf("Signature: %q", signature)

}
