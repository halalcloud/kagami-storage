package meta

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"kagamistoreage/libs/errors"
	"net/http"

	log "kagamistoreage/log/glog"
)

const (
	// bit
	StoreStatusEnableBit       = 31
	StoreStatusReadBit         = 0
	StoreStatusWriteBit        = 1
	StoreStatusRecoverBit      = 2
	StoreStatusRecoverdoingBit = 3
	// status
	StoreStatusInit         = 0
	StoreStatusEnable       = (1 << StoreStatusEnableBit)
	StoreStatusRead         = StoreStatusEnable | (1 << StoreStatusReadBit)
	StoreStatusWrite        = StoreStatusEnable | (1 << StoreStatusWriteBit)
	StoreStatusRecover      = StoreStatusEnable | (1 << StoreStatusRecoverBit)
	StoreStatusRecoverDoing = StoreStatusEnable | (1 << StoreStatusRecoverdoingBit)
	StoreStatusHealth       = StoreStatusRead | StoreStatusWrite
	StoreStatusFail         = StoreStatusEnable
	// api
	statAPI         = "http://%s/info"
	getAPI          = "http://%s/get?key=%d&cookie=%d&vid=%d"
	probeAPI        = "http://%s/probe?vid=%d"
	_probegetkeyAPI = "http://%s/probegetkey?vid=%d"
	_probekeyAPI    = "http://%s/probekey?vid=%d&keys=%s"
	_checkpingApi   = "http://%s/checkping"
)

type StoreList []*Store

func (sl StoreList) Len() int {
	return len(sl)
}

func (sl StoreList) Less(i, j int) bool {
	return sl[i].Id < sl[j].Id
}

func (sl StoreList) Swap(i, j int) {
	sl[i], sl[j] = sl[j], sl[i]
}

// store zk meta data.
type Store struct {
	Stat      string `json:"stat"`
	Admin     string `json:"admin"`
	Api       string `json:"api"`
	Rebalance string `json:rebalance`
	Id        string `json:"id"`
	Rack      string `json:"rack"`
	Status    int    `json:"status"`
}

type VolInfo struct {
	Dirpath  string   `json:"dirpath"`
	Vids     []string `json:"vids"`
	Freevids []string `json:"freevids"`
}

type Recoverystat struct {
	Srcstoreid    string `json:"srcstoreid"`
	ReStatus      int    `json:"restatus"`
	MoveTotalData int64  `json:"movetotaldata"`
	MoveData      int64  `json:"movedata"`
	Utime         int64  `json:"utime"`
}

type ProbeFilekeys struct {
	Filekeys []string `json:"filekeys"`
}

func (s *Store) String() string {
	return fmt.Sprintf(`	
-----------------------------
Id:     %s
Stat:   %s
Admin:  %s
Api:    %s
Rack:   %s
Status: %d
-----------------------------
`, s.Id, s.Stat, s.Admin, s.Api, s.Rack, s.Status)
}

// statAPI get stat http api.
func (s *Store) statAPI() string {
	return fmt.Sprintf(statAPI, s.Stat)
}

// getApi get file http api
func (s *Store) getAPI(n *Needle, vid int32) string {
	return fmt.Sprintf(getAPI, s.Stat, n.Key, n.Cookie, vid)
}

// probeApi probe store
func (s *Store) probeAPI(vid int32) string {
	return fmt.Sprintf(probeAPI, s.Admin, vid)
}

func (s *Store) checkpingAPI() string {
	return fmt.Sprintf(_checkpingApi, s.Admin)
}

func (s *Store) getprobekeyAPI(vid int32) string {
	return fmt.Sprintf(_probegetkeyAPI, s.Admin, vid)
}

func (s *Store) probekeyAPI(vid int32, keys string) string {
	return fmt.Sprintf(_probekeyAPI, s.Admin, vid, keys)
}

func (s *Store) Getprobekey(vid int32) (filekeys []string) {
	var (
		body []byte
		url  = s.getprobekeyAPI(vid)
		resp *http.Response
		data ProbeFilekeys
		err  error
	)
	if resp, err = http.Get(url); err != nil {
		log.Warningf("http.Get(\"%s\") error(%v)", url, err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		err = errors.ErrInternal
		return
	}
	if body, err = ioutil.ReadAll(resp.Body); err != nil {
		log.Errorf("ioutil.ReadAll() error(%v)", err)
		return
	}
	if err = json.Unmarshal(body, &data); err != nil {
		log.Errorf("json.Unmarshal() error(%v)", err)
		return
	}
	filekeys = data.Filekeys
	return
}

func (s *Store) Probekey(vid int32, filekeys []string) (status int) {
	var (
		//	body            []byte
		keys, fkey, url string
		resp            *http.Response
		//	data            ProbeFilekeys
		err error
		i   int
	)
	status = StoreStatusHealth
	for i, fkey = range filekeys {
		if i == 0 {
			keys = fkey
		} else {
			keys = keys + "," + fkey
		}
	}
	url = s.probekeyAPI(vid, keys)
	if resp, err = http.Get(url); err != nil {
		log.Warningf("http.Get(\"%s\") error(%v)", url, err)
		status = StoreStatusFail
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		//err = errors.ErrInternal
		log.Errorf("storeid %s vid %d probe key failed rescode %d", s.Id, vid, resp.StatusCode)
		if resp.StatusCode == http.StatusNotFound {
			status = StoreStatusRecover
		}
	}
	return
}

// Info get store volumes info.
func (s *Store) Info() (vs []*Volume, err error) {
	var (
		body []byte
		resp *http.Response
		data = new(Volumes)
		url  = s.statAPI()
	)
	if resp, err = http.Get(url); err != nil {
		log.Warningf("http.Get(\"%s\") error(%v)", url, err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		err = errors.ErrInternal
		return
	}
	if body, err = ioutil.ReadAll(resp.Body); err != nil {
		log.Errorf("ioutil.ReadAll() error(%v)", err)
		return
	}
	if err = json.Unmarshal(body, &data); err != nil {
		log.Errorf("json.Unmarshal() error(%v)", err)
		return
	}
	vs = data.Volumes
	return
}

func (s *Store) Checkping() (err error) {
	var (
		resp *http.Response
		url  string
	)
	url = s.checkpingAPI()
	//log.Errorf("url = %s", url)
	if resp, err = http.Get(url); err != nil {
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		err = errors.ErrInternal
	}
	return
}

// Head send a head request to store.
func (s *Store) Head(vid int32) (err error) {
	var (
		resp *http.Response
		url  string
	)
	url = s.probeAPI(vid)
	if resp, err = http.Head(url); err != nil {
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusInternalServerError {
		err = errors.ErrInternal
	}
	return
}

// CanWrite reports whether the store can write.
func (s *Store) CanWrite() bool {
	return s.Status == StoreStatusWrite || s.Status == StoreStatusHealth
}

// CanRead reports whether the store can read.
func (s *Store) CanRead() bool {
	return s.Status == StoreStatusRead || s.Status == StoreStatusHealth
}

func (s *Store) IsFail() bool {
	return s.Status == StoreStatusFail
}
