package auth

import (
	"crypto/hmac"
	"crypto/sha1"
	"efs/authacess/bucket"
	"efs/authacess/conf"
	log "efs/log/glog"
	"encoding/base64"
	"encoding/json"
	"errors"
	//"fmt"
	"hash"
	//"strconv"
	//"reflect"
	"strings"
	"time"
)

const (
	TOKEN_DELIMITER = ":"
)

type Auth struct {
	c       *conf.Config
	ibucket *bucket.BucketInfo
}

type PutPolicy struct {
	Scope               string   `json:"scope"`
	Expires             uint32   `json:"deadline"`             // 截止时间（以秒为单位）
	InsertOnly          uint16   `json:"insertOnly,omitempty"` // 若非0, 即使Scope为 Bucket:Key 的形式也是insert only
	DetectMime          uint8    `json:"detectMime,omitempty"` // 若非0, 则服务端根据内容自动确定 MimeType
	CallbackFetchKey    uint8    `json:"callbackFetchKey,omitempty"`
	FsizeLimit          int64    `json:"fsizeLimit,omitempty"`
	FsizeMin            int64    `json:"fsizeMin,omitempty"`
	MimeLimit           string   `json:"mimeLimit,omitempty"`
	SaveKey             string   `json:"saveKey,omitempty"`
	CallbackUrl         string   `json:"callbackUrl,omitempty"`
	CallbackHost        string   `json:"callbackHost,omitempty"`
	CallbackBody        string   `json:"callbackBody,omitempty"`
	CallbackBodyType    string   `json:"callbackBodyType,omitempty"`
	ReturnUrl           string   `json:"returnUrl,omitempty"`
	ReturnBody          string   `json:"returnBody,omitempty"`
	PersistentOps       string   `json:"persistentOps,omitempty"`
	PersistentNotifyUrl string   `json:"persistentNotifyUrl,omitempty"`
	PersistentPipeline  string   `json:"persistentPipeline,omitempty"`
	AsyncOps            string   `json:"asyncOps,omitempty"`
	EndUser             string   `json:"endUser,omitempty"`
	Checksum            string   `json:"checksum,omitempty"` // 格式：<HashName>:<HexHashValue>，目前支持 MD5/SHA1。
	UpHosts             []string `json:"uphosts,omitempty"`
	DeleteAfterDays     int      `json:"deleteAfterDays,omitempty"`
}

func New(c *conf.Config) (a *Auth, err error) {
	a = &Auth{}
	a.c = c
	a.ibucket = bucket.Bucket_init(c)
	return
}

func (a *Auth) interfacetostring(m map[string]interface{}, key string) (r string) {
	var (
		ok             bool
		interfacevalue interface{}
	)
	interfacevalue, ok = m[key]
	if !ok {
		return ""
	}
	r, ok = interfacevalue.(string)
	if !ok {
		log.Errorf("this key %s is not string type ", key)
		return ""
	}
	return r

}
func (a *Auth) interfacetoint64(m map[string]interface{}, key string) (r int64) {
	var (
		ok             bool
		interfacevalue interface{}
	)
	interfacevalue, ok = m[key]
	if !ok {
		return 0
	}
	r, ok = interfacevalue.(int64)
	if !ok {
		log.Errorf("this key %s is not int64 type ", key)
		return 0
	}
	return r

}
func (a *Auth) interfacetofloat64(m map[string]interface{}, key string) (r float64) {
	var (
		ok             bool
		interfacevalue interface{}
	)
	interfacevalue, ok = m[key]
	if !ok {
		return 0
	}
	r, ok = interfacevalue.(float64)
	if !ok {
		log.Errorf("this key %s is not int type ", key)
		return 0
	}
	return r

}

func (a *Auth) sign(keysecret, encodeputpolicy, encodesign string) bool {
	var (
		mac hash.Hash
	)
	mac = hmac.New(sha1.New, []byte(keysecret))
	mac.Write([]byte(encodeputpolicy))
	sn := base64.URLEncoding.EncodeToString(mac.Sum(nil))
	if sn != encodesign {
		return false
	}
	return true
}

func (a *Auth) UploadAuthorize(token string) (pdata *PutPolicy, uid,
	replication int, ak, keysecret string, err error, retcode int) {
	var (
		ss             = strings.Split(token, ":") //acesskey:encodesign:putpolicy
		putpolicy_data []byte

		acesskey  string
		retcode1  int
		putpolicy PutPolicy
	)
	retcode = 401
	if len(ss) != 3 {
		err = errors.New("token is not 3 part")
		return
	}
	encodeputpolicy := ss[2]
	if encodeputpolicy == "" {
		err = errors.New("token is null")
		return
	}

	token_type := strings.Split(ss[0], " ")
	if len(token_type) == 1 {
		ak = token_type[0]
	} else if len(token_type) == 2 {
		ak = token_type[1]
	} else {
		log.Errorf("token type acesskey not match")
		err = errors.New("acesskey not match")
		return
	}

	putpolicy_data, err = base64.URLEncoding.DecodeString(encodeputpolicy)
	if err != nil {
		log.Errorf("putpolicy base64 decode failed err (%v)", err)
		return
	}

	//解析上传策略接口体
	err = json.Unmarshal(putpolicy_data, &putpolicy)
	if err != nil {
		err = errors.New("putpolicy is invalid")
		return
	}
	pdata = &putpolicy
	deadline := putpolicy.Expires
	now := time.Now().Unix()
	expr := now - int64(deadline)
	if expr > 0 {
		log.Errorf("putpolicy deadline timeout deadlinu: %d now:%d", deadline, now)
		err = errors.New("deadline timeout")
		return
	}

	scope := putpolicy.Scope
	if scope == "" {
		log.Errorf("putpolicy scope(%s) invaild", scope)
		err = errors.New("putpolicy scope invaild")
		return
	}
	ekey := strings.Split(scope, ":")
	if ekey[0] == "" {
		log.Errorf("putpolicy scope(%s) invaild", scope)
		err = errors.New("putpolicy scope invaild")
		return
	}

	acesskey, keysecret, _, _, replication, uid, err,
		retcode1 = a.ibucket.Getkey(ekey[0], ak)
	if err != nil {
		log.Errorf("get bucket")
		retcode = retcode1
		err = errors.New("putpolicy scope invaild")
		return
	}
	//	log.Errorf("ss[0]==%s  accesskey =%s,skey=%s", ss[0], acesskey, keysecret)
	//token type spilt " "

	if ak != acesskey {
		log.Errorf("acesskey not match")
		err = errors.New("acesskey not match")
		return
	}

	success := a.sign(keysecret, encodeputpolicy, ss[1])
	if !success {
		log.Errorf("token secret not match")
		err = errors.New("token secret not match")
		return
	}

	retcode = 200
	return
}

func (a *Auth) ManagerAuthorize(token, data, bucket string) (int, int, bool) {
	var reqAccessKey string
	tokenArr := strings.Split(token, TOKEN_DELIMITER)
	if len(tokenArr) != 2 {
		log.Errorf("token (%s) is not : split", token)
		return 0, -1, false
	}

	token_type := strings.Split(tokenArr[0], " ")
	if len(token_type) == 1 {
		reqAccessKey = token_type[0]
	} else if len(token_type) == 2 {
		reqAccessKey = token_type[1]
	} else {
		log.Errorf("token type acesskey not match")
		return 0, -1, false
	}
	reqSign := tokenArr[1]

	accessKey, secretKey, _, _, replication, uid, err, _ := a.ibucket.Getkey(bucket, reqAccessKey)
	if err != nil || accessKey != reqAccessKey {
		log.Errorf("get bucket %s failed or ak %s local ak %s is not match", bucket, reqAccessKey, accessKey)
		return 0, -1, false
	}

	if reqAccessKey != accessKey {
		log.Errorf("acesskey is not match")
		return replication, uid, false
	}

	if !a.managerSign(secretKey, data, reqSign) {
		log.Errorf("token secret is not match")
		return replication, uid, false
	}

	return replication, uid, true
}

func (a *Auth) ManagerPreFetchAuthorize(token, data, bucket string) (string, int, int, bool) {
	var reqAccessKey string

	tokenArr := strings.Split(token, TOKEN_DELIMITER)
	if len(tokenArr) != 2 {
		return "", 0, -1, false
	}
	token_type := strings.Split(tokenArr[0], " ")
	if len(token_type) == 1 {
		reqAccessKey = token_type[0]
	} else if len(token_type) == 2 {
		reqAccessKey = token_type[1]
	} else {
		log.Errorf("token type acesskey not match")
		return "", 0, -1, false
	}

	reqSign := tokenArr[1]

	accessKey, secretKey, propety, imgSource, replication, uid, err, _ := a.ibucket.Getkey(bucket, reqAccessKey)
	if err != nil || accessKey != reqAccessKey {
		return "", replication, -1, false
	}

	public := a.ibucket.Public(propety, false)
	if public {
		return imgSource, replication, uid, true
	}

	if reqAccessKey != accessKey {
		return "", replication, uid, false
	}

	if !a.managerSign(secretKey, data, reqSign) {
		return "", replication, uid, false
	}

	return imgSource, replication, uid, true
}

func (a *Auth) managerSign(secretKey, data, sign string) bool {
	mac := hmac.New(sha1.New, []byte(secretKey))
	mac.Write([]byte(data))
	if base64.URLEncoding.EncodeToString(mac.Sum(nil)) != sign {
		return false
	}

	return true
}
