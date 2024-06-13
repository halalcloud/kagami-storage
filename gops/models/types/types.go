package types

import (
	"efs/libs/errors"
	"efs/libs/meta"
	"efs/libs/stat"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/astaxie/beego"
	//	log "github.com/golang/glog"
)

const (
	statAPI = "http://%s/info"
)

type Store struct {
	Id          string   `json:"id"`
	Ip          string   `json:"ip"`
	Api         string   `json:"api"`
	Stat        string   `json:"stat"`
	Admin       string   `json:"admin"`
	Rebalance   string   `json:rebalance`
	Rack        string   `json:"rack"`
	Status      int      `json:"status"`
	Volumes     []uint64 `json:"volumes"`
	Freevolumes int      `json:"free_volumes"`
}

type Rack struct {
	Name   string   `json:"name"`
	Stores []*Store `json:"stores"`
}

type Group struct {
	Id       uint64   `json:"id"`
	StoreIds []string `json:"storeIds"`
	Volumes  []uint64 `json:volumes`
	Stores   []*Store `json:"stores"`
	Status   int      `json:"status"`
}

type Volume struct {
	Id                  uint64   `json:"id"`
	TotalWriteProcessed uint32   `json:"total_write_processed"`
	TotalWriteDelay     uint32   `json:"total_write_processed"`
	FreeSpace           uint64   `json:"free_space"`
	StoreIds            []string `json:"storeIds"`
	//Status              int      `json:"status"`
	Status      map[string]int `json:"status"`
	Badstoreids []string       `json:"bad_store_ids"`
	Delnums     int32          `json:"del_nums"`
}

type Free_volumes struct {
	Free_volumes int `json:"free_volumes"`
}

type server struct {
	Server *stat.Info `json:"server"`
}

type JsonResponse struct {
	code int
	data interface{}
}

// statAPI get stat http api.
func (s *Store) statAPI() string {
	return fmt.Sprintf(statAPI, s.Stat)
}

// Info get store info.
func (s *Store) Sinfo() (vs []*meta.Volume, fvs int, info *stat.Info, err error) {
	var (
		body []byte
		resp *http.Response
		url  = s.statAPI()
		//	obj  interface{}
		fvdata = new(Free_volumes)
		vdata  = new(meta.Volumes)
		sdata  = new(server)
	)

	if resp, err = http.Get(url); err != nil {
		beego.Error(err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		err = errors.ErrInternal
		return
	}
	if body, err = ioutil.ReadAll(resp.Body); err != nil {
		beego.Error(err)
		return
	}
	if err = json.Unmarshal(body, &vdata); err != nil {
		panic(err)
		beego.Error(err)
		return
	}
	vs = vdata.Volumes
	//fmt.Println(vs)
	if err = json.Unmarshal(body, &sdata); err != nil {
		panic(err)
		beego.Error(err)
		return
	}
	info = sdata.Server
	//beego.Info(info)
	//fmt.Println(string(body))
	if err = json.Unmarshal(body, &fvdata); err != nil {
		panic(err)
		beego.Error(err)
		return
	}
	fvs = fvdata.Free_volumes
	//fmt.Println(fvdata.Free_volumes)
	return
}

//store sort by id
type StorebyId []*Store

func (s StorebyId) Len() int           { return len(s) }
func (s StorebyId) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s StorebyId) Less(i, j int) bool { return s[i].Id < s[j].Id }

//group sort by id
type GroupbyId []*Group

func (g GroupbyId) Len() int           { return len(g) }
func (g GroupbyId) Swap(i, j int)      { g[i], g[j] = g[j], g[i] }
func (g GroupbyId) Less(i, j int) bool { return g[i].Id < g[j].Id }

//volume sort by id
type VolumebyId []*Volume

func (v VolumebyId) Len() int           { return len(v) }
func (v VolumebyId) Swap(i, j int)      { v[i], v[j] = v[j], v[i] }
func (v VolumebyId) Less(i, j int) bool { return v[i].Id < v[j].Id }
