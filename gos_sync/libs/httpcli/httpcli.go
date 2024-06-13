package httpcli

import (
	"net/http"
	"time"
)

var (
	tr = &http.Transport{
		MaxIdleConns:       10,
		IdleConnTimeout:    60 * time.Second,
		DisableCompression: true,
	}

	client = &http.Client{
		Transport: tr,
		Timeout:   60 * time.Second,
	}
)

func Req(req *http.Request) (resp *http.Response, err error) {
	if resp, err = client.Do(req); err != nil {
		return
	}

	return
}
