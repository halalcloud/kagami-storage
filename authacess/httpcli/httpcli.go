package httpcli

import (
	log "efs/log/glog"
	"net"
	"net/http"
	"time"
)

const MAX_RETRY = 2

var (
	_transport = &http.Transport{
		Dial: func(netw, addr string) (c net.Conn, err error) {
			if c, err = net.DialTimeout(netw, addr, 5*time.Second); err != nil {
				return nil, err
			}
			return c, nil
		},
		DisableCompression: true,
	}
	_client = &http.Client{
		Transport: _transport,
		Timeout:   10 * time.Second,
	}
)

func HttpReq_del(req *http.Request) (resp *http.Response, err error) {
	if resp, err = _client.Do(req); err != nil {
		log.Errorf("_client.Do(%s) error(%v)", req.URL.Path, err)
		return
	}

	return
}

func HttpReq(req *http.Request) (resp *http.Response, err error) {
	for i := 1; i <= MAX_RETRY; i++ {
		if resp, err = _client.Do(req); err == nil {
			break
		}
		time.Sleep(time.Second)
	}
	if err != nil {
		log.Errorf("_client.Do(%s) error(%v)", req.URL.Path, err)
		return
	}

	return
}
