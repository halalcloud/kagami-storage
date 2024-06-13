package auth

import (
	"crypto/hmac"
	"crypto/sha1"

	"encoding/base64"
	"kagamistoreage/bucket/conf"

	//	"fmt"

	"hash"

	"strings"
)

const (
	TOKEN_DELIMITER = ":"
)

type Auth struct {
	c *conf.Config
}

func New(c *conf.Config) (a *Auth) {
	a = &Auth{}
	a.c = c
	return
}

func (a *Auth) BucketAuthorize(token, data string) bool {
	tokenArr := strings.Split(token, TOKEN_DELIMITER)
	if len(tokenArr) != 2 {
		return false
	}
	reqAccessKey := tokenArr[0]
	reqSign := tokenArr[1]

	accessKey, secretKey := a.c.Ak, a.c.Sk
	if accessKey != reqAccessKey {
		return false
	}

	if !a.sign(secretKey, data, reqSign) {
		return false
	}

	return true
}

func (a *Auth) sign(keysecret, data, encodesign string) bool {
	var (
		mac hash.Hash
	)
	mac = hmac.New(sha1.New, []byte(keysecret))
	mac.Write([]byte(data))
	if base64.URLEncoding.EncodeToString(mac.Sum(nil)) != encodesign {
		return false
	}
	return true
}
