package upload_strategy

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha1"
	"efs/authacess/httpcli"
	"efs/authacess/variable"
	log "efs/log/glog"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"hash"
	"io"
	"io/ioutil"
	"mime/multipart"
	"net"
	"net/http"
	"net/url"
	//"strconv"
	"strings"
	"time"

	itime "github.com/Terry-Mao/marmot/time"
)

const (
	ResOk                = 200
	E_BadMessage         = 400
	E_TokenInvalid       = 401
	E_DataToolong        = 413
	E_CallbackFail       = 579
	E_ServerFail         = 599
	E_DestSourceExists   = 614
	_upload_content_type = "multipart/form-data"
	_redirect_url        = "%s?upload_ret=%s"
)

var (
	_timer     = itime.NewTimer(1024)
	_transport = &http.Transport{
		Dial: func(netw, addr string) (c net.Conn, err error) {
			if c, err = net.DialTimeout(netw, addr, 2*time.Second); err != nil {
				return nil, err
			}
			return c, nil
		},
		DisableCompression: true,
	}
	_client = &http.Client{
		Transport: _transport,
	}
	_canceler = _transport.CancelRequest
)

func Http(method, uri string, params url.Values, header url.Values, buf []byte) (rbody []byte, err error, code int) {
	var (
		body    []byte
		w       *multipart.Writer
		bw      io.Writer
		bufdata = &bytes.Buffer{}
		req     *http.Request
		resp    *http.Response
		ru      string
		enc     string
		ctype   string
	)
	enc = params.Encode()
	if enc != "" {
		ru = uri + "?" + enc
	}

	if method == "GET" {
		if req, err = http.NewRequest("GET", ru, nil); err != nil {
			return
		}
	} else {
		if buf == nil {
			if req, err = http.NewRequest("POST", uri, strings.NewReader(enc)); err != nil {
				return
			}
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		} else {
			w = multipart.NewWriter(bufdata)
			if bw, err = w.CreateFormFile("file", "1.jpg"); err != nil {
				return
			}

			if _, err = bw.Write(buf); err != nil {
				return
			}

			for key, _ := range params {
				w.WriteField(key, params.Get(key))
			}
			ctype = w.FormDataContentType()
			if err = w.Close(); err != nil {
				return
			}
			if req, err = http.NewRequest("POST", uri, bufdata); err != nil {
				return
			}
			req.Header.Set("Content-Type", ctype)
		}
	}

	for key, _ := range header {
		req.Header.Set(key, header.Get(key))
	}
	td := _timer.Start(5*time.Second, func() {
		_canceler(req)
	})
	if resp, err = _client.Do(req); err != nil {
		log.Errorf("_client.Do(%s) error(%v)", ru, err)
		return
	}
	td.Stop()
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Errorf("_client.Do(%s) status: %d", ru, resp.StatusCode)
		code = resp.StatusCode
	}
	if body, err = ioutil.ReadAll(resp.Body); err != nil {
		log.Errorf("ioutil.ReadAll(%s) uri(%s) error(%v)", body, ru, err)
		return
	}
	rbody = body
	return
	/*
		if err = json.Unmarshal(body, res); err != nil {
			log.Errorf("json.Unmarshal(%s) uri(%s) error(%v)", body, ru, err)
		}
		return
	*/
}

func interfacetostring(m map[string]interface{}, key string) (r string) {
	var (
		ok             bool
		interfacevalue interface{}
	)
	interfacevalue, ok = m[key]
	if !ok {
		//log.Errorf("map have no this key:%s", key)
		return ""
	}
	r, ok = interfacevalue.(string)
	if !ok {
		log.Errorf("this key %s is not string type ", key)
		return ""
	}
	return r

}

func interfacetoint(m map[string]interface{}, key string) (r int) {
	var (
		ok             bool
		interfacevalue interface{}
		ret            float64
	)
	interfacevalue, ok = m[key]
	if !ok {
		//		log.Errorf("map have no this key:%s", key)
		return 0
	}
	//log.Errorf("key type %T,++++++ %+v", interfacevalue, m)
	ret, ok = interfacevalue.(float64)
	if !ok {
		log.Errorf("this key %s is not string type ", key)
		return -1
	}
	r = int(ret)
	return

}

/*
func Upload_getkey(putpolicy []byte, key string, v *variable.Variable) (rkey, errstring string, retcode int) {
	var (
		putpolicydata map[string]interface{}
		err           error
	)
	retcode = 200
	err = json.Unmarshal(putpolicy, &putpolicydata)
	if err != nil {
		log.Errorf("putpolicy json decode failed err (%v)", err)
		retcode = E_BadMessage
		errstring = "json decode putpolicy failed"
		return
	}

	if key == "" {
		//	imsage_key := interfacetostring(putpolicydata, "saveKey")
		tk := interfacetostring(putpolicydata, "saveKey")
		if tk != "" {
			if tk[:4] == "$(x:" {
				rkey = v.Getcustomvariable(tk)
			} else {
				if tk[:2] == "$(" {
					rkey = v.Getmagicvariable(tk)
				}
			}
			return
		}

	}
	rkey = key
	return

}
*/

func Upload_isoverwrite(InsertOnly uint16, filename string) (overwrite int) {
	if filename != "" {
		overwrite = 1
	}

	if InsertOnly != 0 {
		overwrite = 0
	}
	return
}

func Upload_returnurl(wr http.ResponseWriter, r *http.Request, returnurl, returnbody string, v *variable.Variable) (retcode int, errstring string, rbody []byte) {
	var (
		r1              map[string]interface{}
		err             error
		direct_url, val string
		body            []byte
		ok              bool
	)
	retcode = 0
	if returnbody != "" {
		err = json.Unmarshal([]byte(returnbody), &r1)
		if err != nil {
			log.Errorf("putpolicy json decode failed err (%v)", err)
			retcode = E_BadMessage
			errstring = "json decode putpolicy failed"
			return
		}
		//r1, ok = interfacebody.(map[string]interface{})

		for k, tval := range r1 {
			if val, ok = tval.(string); ok {
				if val[:4] == "$(x:" {
					r1[k] = v.Getcustomvariable(val[4:(len(val) - 1)])
				} else {
					if val[:2] == "$(" {
						r1[k] = v.Getmagicvariable(val)
					}
				}
			}

		}
		body, err = json.Marshal(r1)
		if err != nil {
			log.Errorf("upload response marshal json error(%v)", err)
			retcode = E_BadMessage
			errstring = "json encode returnbody failed"
			return
		}
	}

	if returnurl != "" {
		if returnbody != "" {

			data := base64.URLEncoding.EncodeToString(body)
			direct_url = fmt.Sprintf(_redirect_url, returnurl, data)
		} else {
			direct_url = returnurl
		}
		http.Redirect(wr, r, direct_url, 303)
		retcode = 303
		return
	} else {
		if returnbody != "" {
			rbody = body
			retcode = 200
		}

	}

	return

}

type ResCallbakErr struct {
	Callbak_url      string `json:"callbak_url"`
	Callbak_bodyType string `json:"callbak_bodyType"`
	Callbak_body     string `json:"callbak_body"`
	Token            string `json:"token"`
	Err_code         int    `json:"err_code"`
	Error            string `json:"error"`
	Hash             string `json:"hash"`
	Key              string `json:"key"`
}

func gettoken_callbak(uri, body, ak, sk string) (token string) {
	var (
		mac hash.Hash
	)

	u, err := url.Parse(uri)
	if err != nil {
		log.Errorf("parse callbakurl failed %s", uri)
		return ""
	}
	data := u.Path + "\n" + body

	mac = hmac.New(sha1.New, []byte(sk))
	mac.Write([]byte(data))
	edata := base64.URLEncoding.EncodeToString(mac.Sum(nil))
	token = "Efs " + ak + edata
	return

}

func Upload_callbak(callbakurl, callbackHost, callbakbody, callbackBodyType string,
	v *variable.Variable, ak, sk string) (retcode int, errstring string, rbody []byte, cbkerres ResCallbakErr) {
	var (
		err        error
		data       []byte
		uri, token string
		req        *http.Request
		resp       *http.Response
	)
	retcode = 579

	callurls := strings.Split(callbakurl, ";")

	reqbody := map[string]interface{}{}
	if callbackBodyType == "application/json" {
		callbodys := map[string]string{}
		if err = json.Unmarshal([]byte(callbakbody), &callbodys); err != nil {
			log.Errorf("upload_callbak json decode err:%s\n", err)
			errstring = "callbakurl failed"
			cbkerres.Callbak_url = uri
			cbkerres.Callbak_bodyType = callbackBodyType
			cbkerres.Callbak_body = string(data)
			cbkerres.Token = token
			cbkerres.Err_code = retcode
			cbkerres.Error = string(rbody)
			return
		}

		for k, vo := range callbodys {
			if vo[:4] == "$(x:" {
				reqbody[k] = v.Getcustomvariable(vo[4:(len(vo) - 1)])
			} else if vo[:2] == "$(" {
				reqbody[k] = v.Getmagicvariable(vo)
			}
		}

		if data, err = json.Marshal(reqbody); err != nil {
			log.Errorf("json encode data err:%s\n", err)
			errstring = "callbakurl failed"
			cbkerres.Callbak_url = uri
			cbkerres.Callbak_bodyType = callbackBodyType
			cbkerres.Callbak_body = string(data)
			cbkerres.Token = token
			cbkerres.Err_code = retcode
			cbkerres.Error = string(rbody)
			return
		}

		for _, uri = range callurls {
			token = gettoken_callbak(uri, string(data), ak, sk)
			req, _ = http.NewRequest("POST", uri, bytes.NewReader(data))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", token)
			if resp, err = httpcli.HttpReq(req); err != nil {
				log.Errorf("httpReq url:%s err:%s\n", uri, err)
				continue
			}
			if resp.StatusCode != http.StatusOK {
				log.Errorf("httpReq url:%s status wrong:%d\n", uri, resp.StatusCode)
				continue
			}

			defer resp.Body.Close()
			break
		}

		if err != nil || resp.StatusCode != http.StatusOK {
			log.Errorf("Upload_callbak httpReq err:%s\n", err)
			errstring = "callbakurl failed"
			cbkerres.Callbak_url = uri
			cbkerres.Callbak_bodyType = callbackBodyType
			cbkerres.Callbak_body = string(data)
			cbkerres.Token = token
			cbkerres.Err_code = retcode
			cbkerres.Error = string(rbody)
			return
		}

		if rbody, err = ioutil.ReadAll(resp.Body); err != nil {
			log.Errorf("read resp data err:%s\n", err)
			errstring = "callbakurl failed"
			cbkerres.Callbak_url = uri
			cbkerres.Callbak_bodyType = callbackBodyType
			cbkerres.Callbak_body = string(data)
			cbkerres.Token = token
			cbkerres.Err_code = retcode
			cbkerres.Error = string(rbody)
			return
		}

		retcode = http.StatusOK
	} else {
		//url param
	}

	return
}

func Upload_callbak_del(callbakurl, callbakbody, callbackBodyType, callbackHost string,
	v *variable.Variable, ak, sk string) (retcode int, errstring string, rbody []byte, cbkerres ResCallbakErr) {
	var (
		err        error
		params     = url.Values{}
		header     = url.Values{}
		cb         []string
		uri, token string
	)
	retcode = 0

	if callbakurl == "" || callbakbody == "" {
		return
	}
	if callbackBodyType == "" {
		header.Set("Content-Type", "application/json")
	} else {
		header.Set("Content-Type", callbackBodyType)
	}
	callurl := strings.Split(callbakurl, ";")
	callbody := strings.Split(callbakbody, "&")
	for _, parstring := range callbody {
		partmp := strings.Split(parstring, "=")
		//TODO magic value
		tk := partmp[1]
		if tk[:4] == "$(x:" {
			//params.Set(partmp[0], v.Getcustomvariable(tk[4:(len(tk)-1)]))
			cb = append(cb, partmp[0]+"="+v.Getcustomvariable(tk[4:(len(tk)-1)]))
		} else {
			if tk[:2] == "$(" {
				//params.Set(partmp[0], v.Getcustomvariable(tk[2:(len(tk)-1)]))
				//cb = append(cb, partmp[0]+"="+v.Getmagicvariable(tk))
			} else {
				//	params.Set(partmp[0], tk)
				cb = append(cb, parstring)
			}

		}
	}
	cbody := strings.Join(cb, "&")

	for _, uri = range callurl {
		//rbody, err, retcode = Http("POST", uri, params, header, nil)
		token = gettoken_callbak(uri, cbody, ak, sk)
		header.Set("Authorization", token)
		header.Set("Content-Type", "application/x-www-form-urlencoded")
		rbody, err, retcode = Http("POST", uri, params, header, []byte(cbody))
		if retcode != 200 {
			continue
		}
		break
	}

	if retcode != 200 {
		log.Errorf("callbakurl failed error(%v)", err)

		errstring = "callbakurl failed"
		cbkerres.Callbak_url = uri
		cbkerres.Callbak_bodyType = callbackBodyType
		cbkerres.Callbak_body = cbody
		cbkerres.Token = token
		cbkerres.Err_code = retcode
		cbkerres.Error = string(rbody)

		retcode = E_CallbackFail

		return
	}

	//fetchkey TODO
	return
}
