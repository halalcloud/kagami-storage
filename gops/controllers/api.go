package controllers

import (
	"crypto/md5"
	"efs/gops/models/almrec"
	"efs/gops/models/global"
	"efs/gops/models/oplog"
	"efs/gops/models/ops"
	"efs/gops/models/sstat"
	"efs/gops/models/types"
	"efs/gops/models/user"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/astaxie/beego"
	"io/ioutil"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

type ApiController struct {
	beego.Controller
}

func (c *ApiController) Initialization() {

}

func (c *ApiController) Login() {
	c.pre()
	if c.islogin() {
		c.Redirect("/rack", 302)
		return
	}

	acount := c.GetString("username")
	passwd := c.GetString("password")
	if acount == "" || passwd == "" {
		c.responseError(errors.New("username or password empty"), 1001)
		return
	}

	userD, err := user.UserbyAcount(acount)
	if err != nil || userD == nil {
		c.responseError(errors.New("username or password no find error"), 1001)
		return
	}

	enc_passwd := c.encPasswd(passwd, acount)
	if enc_passwd != userD.Password {
		c.responseError(errors.New("username or password enc error"), 1001)
		return
	}

	userData := map[string]string{"last_login": time.Now().Format("2006-01-02 15:04:05")}
	if rs, err1 := user.UpdatebyID(userData, userD.ID); err1 != nil || rs == false {
		c.responseError(errors.New("update user last_login error"), 1001)
		return
	}

	var respM = make(map[string]interface{})
	respM["token"] = enc_passwd
	respM["uid"] = userD.ID
	respM["stat"] = userD.Stat

	user.LoginUsers[enc_passwd] = strconv.Itoa(int(userD.ID)) + "-" +
		strconv.Itoa(int(time.Now().Unix()))

	c.responseOk(respM)
	return
}

func (c *ApiController) OverView() {
	c.pre()
	if !c.islogin() {
		c.responseError(errors.New("login first, please"), 1001)
	}
	totalSpace, totalFreeSpace := getCapacity()
	racks, stores := getRackAndStoreNum()

	var respM = make(map[string]interface{})
	respM["total"] = totalSpace
	respM["used"] = totalSpace - totalFreeSpace
	respM["racks"] = racks
	respM["stores"] = stores
	c.responseOk(respM)
}

func getCapacity() (totalSpace, totalFreeSpace uint64) {
	groups := ops.OpsManager.GetGroup()
	global.Volume_lock.RLock()
	volumes := global.VOLUMES
	global.Volume_lock.RUnlock()

	for _, g := range groups {
		totalSpace += uint64(len(g.Volumes)) * global.VOLUME_SPACE
		for _, vid := range g.Volumes {
			if _, ok := volumes[vid]; !ok {
				beego.Error("getcapacity vid:", vid, "no exist in volumes(", volumes, ")")
			} else {
				totalFreeSpace += (volumes[vid].FreeSpace * 8)
			}
		}
	}

	return
}

func getRackAndStoreNum() (racks, stores int) {
	rackm := ops.OpsManager.GetRack()
	racks = len(rackm)

	for _, vo := range rackm {
		stores += len(vo)
	}

	return
}

type statItem struct {
	Gid  uint64 `json:"gid"`
	Rack string `json:"rack"`
	IP   string `json:"ip"`
	Desc string `json:"desc"`
}

func (c *ApiController) StatOverView() {
	c.pre()
	if !c.islogin() {
		c.responseError(errors.New("login first, please"), 1001)
	}

	var stats []*statItem

	groups := ops.OpsManager.GetGroup()
	stores := global.STORES
	volumes := global.VOLUMES

	for _, group := range groups {
		for _, sid := range group.StoreIds {
			store, ok := stores[sid]
			if !ok {
				beego.Error("statoverview  sid:", sid, " no exist")
				continue
			}

			if store.Status == global.Statusfail {
				//store
				stat := new(statItem)
				stat.Gid, stat.Rack, stat.IP = group.Id, store.Rack, store.Ip
				stat.Desc = "store error"
				stats = append(stats, stat)
			} else {
				//volume
				desc := ""
				for _, vid := range store.Volumes {
					volume, ok := volumes[vid]
					if !ok {
						beego.Error("statoverview  vid:", vid, " no exist")
						continue
					}
					if volume.Status[sid] == global.Statusfail {
						desc += "," + strconv.FormatUint(vid, 10)
					}
				}
				if desc != "" {
					stat := new(statItem)
					stat.Gid, stat.Rack, stat.IP = group.Id, store.Rack, store.Ip
					stat.Desc = desc + " error"
					stats = append(stats, stat)
				}
			}
		}
	}

	c.responseOk(stats)
	return
}

func (c *ApiController) GetRack() {
	c.pre()
	if !c.islogin() {
		c.responseError(errors.New("login first, please"), 1001)
		return
	}

	page, err := c.GetInt("page")
	if err != nil || page <= 0 {
		page = 1
	}
	pagenum, err := c.GetInt("pagenum")
	if err != nil || pagenum <= 0 {
		pagenum = 20
	}
	hnum := (page - 1) * pagenum
	tnum := hnum + pagenum

	var rnames []string
	rackm := ops.OpsManager.GetRack()

	if len(rackm) <= hnum {
		c.responseError(errors.New("no record"), 1010)
		return
	}

	for rname, _ := range rackm {
		rnames = append(rnames, rname)
	}
	sort.Strings(rnames)

	var respM map[string]interface{} = make(map[string]interface{})
	respM["amount"] = len(rnames)

	if len(rnames) >= tnum {
		respM["data"] = rnames[hnum:tnum]
	} else {
		respM["data"] = rnames[hnum:]
	}

	c.responseOk(respM)
	return
}

func (c *ApiController) GetRackStore() {
	c.pre()
	if !c.islogin() {
		c.responseError(errors.New("login first, please"), 1001)
		return
	}

	page, err := c.GetInt("page")
	if err != nil || page <= 0 {
		page = 1
	}
	pagenum, err := c.GetInt("pagenum")
	if err != nil || pagenum <= 0 {
		pagenum = 20
	}
	hnum := (page - 1) * pagenum
	tnum := hnum + pagenum

	rname := c.GetString("rname")
	rackm := ops.OpsManager.GetRack()

	stores, ok := rackm[rname]
	if !ok || len(stores) <= hnum {
		c.responseError(errors.New("no record"), 1010)
		return
	}
	//store sort by id
	sort.Sort(types.StorebyId(stores))

	var respM map[string]interface{} = make(map[string]interface{})
	respM["amount"] = len(stores)

	if len(stores) >= tnum {
		respM["data"] = stores[hnum:tnum]
	} else {
		respM["data"] = stores[hnum:]
	}

	c.responseOk(respM)
	return
}

func (c *ApiController) GetStoreDevice() {
	c.pre()
	if !c.islogin() {
		c.responseError(errors.New("login first, please"), 1001)
		return
	}
	storeid := c.GetString("storeid")
	if storeid == "" {
		c.responseError(errors.New("wrong store id"), 1010)
		return
	}
	store, ok := global.STORES[storeid]
	if !ok {
		c.responseError(errors.New("store no exist"), 1010)
		return
	}
	url := "http://" + store.Stat + "/diskinfo"
	//url = "http://192.168.100.98:6061/diskinfo"
	ret, err := c.httpget(url)
	if err != nil {
		c.responseError(errors.New("get store device fail"), 1010)
		return
	}

	resps := make([]map[string]interface{}, 0)
	data := make(map[string]interface{})
	json.Unmarshal([]byte(ret), &data)
	for k, v := range data {
		switch tv := v.(type) {
		case []interface{}:
			if k != "diskinfo" {
				continue
			}
			for _, vo := range tv {
				resp := make(map[string]interface{})
				tvo := vo.(map[string]interface{})
				resp["dev_name"] = tvo["devname"]
				resp["mount_point"] = tvo["mountpoint"]
				resp["free_space"] = tvo["avail_space"]
				tvolume, err := calVolume(tvo["avail_space"].(string))
				resp["free_volume"] = tvolume
				if err != nil {
					c.responseError(err, 1010)
					return
				}
				resps = append(resps, resp)
			}
		}
	}

	c.responseOk(resps)
}

func (c *ApiController) GetStoreQps() {
	c.pre()
	if !c.islogin() {
		c.responseError(errors.New("login first, please"), 1001)
		return
	}

	storeid := c.GetString("storeid")
	if storeid == "" {
		c.responseError(errors.New("store id error"), 1010)
		return
	}

	var recAcount int32
	interval := c.GetString("interval")
	switch interval {
	case "h":
		recAcount = 720
	case "d":
		recAcount = 17280
	case "w":
		recAcount = 120960
	default:
		recAcount = 720
	}

	qpss, err := sstat.QPSbyCon(storeid, recAcount)
	if err != nil {
		c.responseError(errors.New("get qps error"), 1010)
		return
	}

	var sid string
	ctimes := make([]string, 0)
	uploads := make([]int32, 0)
	downloads := make([]int32, 0)
	dels := make([]int32, 0)
	for _, v := range qpss {
		sid = v.StoreID
		ctimes = append(ctimes, v.Ctime)
		uploads = append(uploads, v.Upload)
		downloads = append(downloads, v.Download)
		dels = append(dels, v.Del)
	}

	resp := make(map[string]interface{})
	resp["storeid"] = sid
	resp["ctimes"] = ctimes
	resp["uploads"] = uploads
	resp["downloads"] = downloads
	resp["dels"] = dels
	c.responseOk(resp)
	return
}

func (c *ApiController) GetStoreTp() {
	c.pre()
	if !c.islogin() {
		c.responseError(errors.New("login first, please"), 1001)
		return
	}

	storeid := c.GetString("storeid")
	if storeid == "" {
		c.responseError(errors.New("store id error"), 1010)
		return
	}

	var recAcount int32
	interval := c.GetString("interval")
	switch interval {
	case "h":
		recAcount = 720
	case "d":
		recAcount = 17280
	case "w":
		recAcount = 120960
	default:
		recAcount = 720
	}

	tps, err := sstat.ThroughputbyCon(storeid, recAcount)
	if err != nil {
		c.responseError(errors.New("get qps error"), 1010)
		return
	}

	var sid string
	ctimes := make([]string, 0)
	tpIns := make([]int32, 0)
	tpOuts := make([]int32, 0)
	for _, v := range tps {
		sid = v.StoreID
		ctimes = append(ctimes, v.Ctime)
		tpIns = append(tpIns, v.Tpin)
		tpOuts = append(tpOuts, v.Tpout)
	}

	resp := make(map[string]interface{})
	resp["storeid"] = sid
	resp["ctimes"] = ctimes
	resp["tpins"] = tpIns
	resp["tpouts"] = tpOuts
	c.responseOk(resp)
	return
}

func (c *ApiController) GetStoreDelay() {
	c.pre()
	if !c.islogin() {
		c.responseError(errors.New("login first, please"), 1001)
		return
	}

	storeid := c.GetString("storeid")
	if storeid == "" {
		c.responseError(errors.New("store id error"), 1010)
		return
	}

	var recAcount int32
	interval := c.GetString("interval")
	switch interval {
	case "h":
		recAcount = 720
	case "d":
		recAcount = 17280
	case "w":
		recAcount = 120960
	default:
		recAcount = 720
	}

	delays, err := sstat.DelaybyCon(storeid, recAcount)
	if err != nil {
		c.responseError(errors.New("get qps error"), 1010)
		return
	}

	var sid string
	ctimes := make([]string, 0)
	uploads := make([]int32, 0)
	downloads := make([]int32, 0)
	dels := make([]int32, 0)
	for _, v := range delays {
		sid = v.StoreID
		ctimes = append(ctimes, v.Ctime)
		uploads = append(uploads, v.Upload)
		downloads = append(downloads, v.Download)
		dels = append(dels, v.Del)
	}

	resp := make(map[string]interface{})
	resp["storeid"] = sid
	resp["ctimes"] = ctimes
	resp["uploads"] = uploads
	resp["downloads"] = downloads
	resp["dels"] = dels
	c.responseOk(resp)
	return
}

//group resp type
type respGroup struct {
	Id              uint64   `json:"id"`
	StoreIps        []string `json:"store_ips"`
	Status          int      `json:"status"`
	AvailFreeVolume int      `json:"avail_free_volume"`
	TotalSpace      string   `json:"total_space"` //Gigabyte
	FreeSpace       string   `json:"free_space"`  //byte
}
type respGroupbyId []*respGroup

func (r respGroupbyId) Len() int           { return len(r) }
func (r respGroupbyId) Swap(i, j int)      { r[i], r[j] = r[j], r[i] }
func (r respGroupbyId) Less(i, j int) bool { return r[i].Id < r[j].Id }

func (c *ApiController) GetGroup() {
	c.pre()
	if !c.islogin() {
		c.responseError(errors.New("login first, please"), 1001)
		return
	}

	volumeReserve, vnErr := beego.AppConfig.Int("VolumeReserve")
	if vnErr != nil {
		c.responseError(errors.New("get volume reserve config error"), 1010)
		return
	}

	page, err := c.GetInt("page")
	if err != nil || page <= 0 {
		page = 1
	}
	pagenum, err := c.GetInt("pagenum")
	if err != nil || pagenum <= 0 {
		pagenum = 20
	}
	hnum := (page - 1) * pagenum
	tnum := hnum + pagenum

	status, err := c.GetInt("status")
	if err != nil {
		status = -1
	}

	groups := ops.OpsManager.GetGroup()
	//var totalSpace, totalAvailSpace uint64
	var rgbyId respGroupbyId
	for _, g := range groups {
		if (status >= 0) && (g.Status != status) {
			continue
		}

		rg := new(respGroup)
		rg.Id = g.Id
		rg.Status = g.Status
		for _, s := range g.Stores {
			rg.StoreIps = append(rg.StoreIps, s.Ip)
		}
		rg.AvailFreeVolume = g.Stores[0].Freevolumes
		for i := 0; i < len(g.Stores); i++ {
			if rg.AvailFreeVolume > g.Stores[i].Freevolumes {
				rg.AvailFreeVolume = g.Stores[i].Freevolumes
			}
		}
		if rg.AvailFreeVolume > volumeReserve {
			rg.AvailFreeVolume = rg.AvailFreeVolume - volumeReserve
		} else {
			rg.AvailFreeVolume = 0
		}

		//total_avail_space; total_space
		tTotalSpace := uint64(len(g.Volumes)) * global.VOLUME_SPACE
		rg.TotalSpace, _ = unitExchange(tTotalSpace)
		global.Volume_lock.RLock()
		volumes := global.VOLUMES
		global.Volume_lock.RUnlock()
		var tgFreeSpace uint64
		for _, vid := range g.Volumes {
			if _, ok := volumes[vid]; !ok {
				beego.Error("vid:", vid, "no exist in volumes(", volumes, ")")
			} else {
				tgFreeSpace += (volumes[vid].FreeSpace * 8)
			}
		}
		rg.FreeSpace, _ = unitExchange(tgFreeSpace) //strconv.FormatUint(tgFreeSpace, 10) + " Byte"

		rgbyId = append(rgbyId, rg)
		//	totalSpace += tTotalSpace
		//	totalAvailSpace += tgFreeSpace
	}
	//group sort by id
	sort.Sort(rgbyId)

	var respM = make(map[string]interface{})
	respM["amount"] = len(rgbyId)
	//respM["total_space"], _ = unitExchange(totalSpace) //strconv.FormatUint(totalSpace, 10) + " GB"
	//respM["total_avail_space"], _ = unitExchange(totalAvailSpace)
	//strconv.FormatUint(totalAvailSpace, 10) + " Byte"
	if len(rgbyId) <= hnum {
		c.responseError(errors.New("no record"), 1010)
		return
	}

	if len(rgbyId) >= tnum {
		respM["groups"] = rgbyId[hnum:tnum]
	} else {
		respM["groups"] = rgbyId[hnum:]
	}

	c.responseOk(respM)
	return
}

func (c *ApiController) GetFreeStore() {
	c.pre()
	if !c.islogin() {
		c.responseError(errors.New("login first, please"), 1001)
		return
	}

	fstores, err := ops.OpsManager.GetFreeStore()
	if err != nil {
		c.responseError(err, 1010)
		return
	}

	rstores := make(map[string][]*types.Store)
	for _, v := range fstores {
		rstores[v.Rack] = append(rstores[v.Rack], v)
	}

	var respM map[string]interface{} = make(map[string]interface{})
	respM["rstore"] = rstores
	respM["racknum"] = len(rstores)
	respM["storenum"] = len(fstores)
	c.responseOk(respM)
	return
}

func (c *ApiController) AddFreeVolume() {
	c.pre()
	if !c.islogin() {
		c.responseError(errors.New("login first, please"), 1001)
		return
	}
	if !c.isAdmin() {
		c.responseError(errors.New("Non administrator, limit action"), 1010)
		return
	}

	var (
		err     error
		storeid string
		bdir    string
		n       int32
	)

	storeid = c.GetString("storeid")
	store, ok := global.STORES[storeid]
	if !ok {
		c.responseError(errors.New("storeid error"), 1010)
		return
	}
	bdir = c.GetString("bdir")
	if bdir == "" {
		c.responseError(errors.New("bdir error"), 1010)
		return
	}
	if n, err = c.GetInt32("n"); err != nil {
		c.responseError(err, 1010)
		return
	}

	if err = ops.OpsManager.AddFreeVolume(store.Admin, n, bdir+"/efsdata", bdir+"/efsdata"); err != nil {
		c.responseError(err, 1010)
		return
	}

	//oplog
	c.addOpLog("add free volume")

	c.responseOk("add ok")
	return
}

func (c *ApiController) AddGroup() {
	c.pre()
	if !c.islogin() {
		c.responseError(errors.New("login first, please"), 1001)
		return
	}

	if !c.isAdmin() {
		c.responseError(errors.New("Non administrator, limit action"), 1010)
		return
	}

	var (
		err    error
		stores []string
		copys  int
		stgy   string
	)

	stores = strings.Split(c.GetString("stores"), ",")
	if isSRept(stores) {
		c.responseError(errors.New("store repeat"), 1010)
		return
	}
	for _, storeId := range stores {
		if global.IsInGroup(storeId) {
			c.responseError(errors.New("store in group"), 1010)
			return
		}
	}

	if copys, err = c.GetInt("copys"); err != nil {
		c.responseError(err, 1010)
		return
	}
	if len(stores) != copys || copys < 1 || copys > 10 {
		c.responseError(errors.New("store or copys is wrong num"), 1010)
		return
	}

	stgy = c.GetString("stgy")
	switch stgy {
	case "rack":
		var racks []string
		global.Store_lock.RLock()
		tStores := global.STORES
		global.Store_lock.RUnlock()
		for _, v := range stores {
			racks = append(racks, tStores[v].Id)
		}

		if isSRept(racks) {
			c.responseError(errors.New("rack repeat"), 1010)
			return
		}
	case "store":
		// no operation
	default:
		c.responseError(errors.New("strategy error"), 1010)
		return
	}

	if err = ops.OpsManager.AddGroup(stores); err != nil {
		c.responseError(err, 1010)
		return
	}

	//oplog
	c.addOpLog("add group")

	c.responseOk("add ok")
	return
}

func (c *ApiController) DelGroup() {
	c.pre()
	if !c.islogin() {
		c.responseError(errors.New("login first, please"), 1001)
		return
	}

	if !c.isAdmin() {
		c.responseError(errors.New("Non administrator, limit action"), 1010)
		return
	}

	groupids := strings.Split(c.GetString("groupids"), ",")
	if len(groupids) <= 0 {
		c.responseError(errors.New("no group be selected"), 1010)
		return
	}

	for _, groupid := range groupids {
		gid, err := strconv.ParseUint(groupid, 10, 64)
		if err != nil {
			c.responseError(err, 1010)
			return
		}

		//if group contains volume
		global.Group_lock.RLock()
		tGroups := global.GROUPS
		global.Group_lock.RUnlock()

		if _, ok := tGroups[gid]; !ok {
			c.responseError(errors.New("group no exist,id: "+groupid), 1010)
			return
		}

		if len(tGroups[gid].Volumes) > 0 {
			c.responseError(errors.New("group no empty,id: "+groupid), 1010)
			return
		}

		if err := ops.OpsManager.DelGroup(gid); err != nil {
			c.responseError(err, 1010)
			return
		}
	}

	//oplog
	c.addOpLog("del group")

	c.responseOk("del ok")
	return
}

//volume resp type
type respVolume struct {
	Id          uint64   `json:"id"`
	StoreIps    []string `json:"store_ips"`
	Status      int      `json:"status"`
	BadStoreIps []string `json:"bad_store_ips"`
	DelNums     uint64   `json:"del_nums"`
	FreeSpace   string   `json:"free_space"`
}
type respVolumebyId []*respVolume

func (r respVolumebyId) Len() int           { return len(r) }
func (r respVolumebyId) Swap(i, j int)      { r[i], r[j] = r[j], r[i] }
func (r respVolumebyId) Less(i, j int) bool { return r[i].Id < r[j].Id }

func (c *ApiController) GetVolume() {
	var (
		isFull bool
	)
	c.pre()
	if !c.islogin() {
		c.responseError(errors.New("login first, please"), 1001)
		return
	}

	page, err := c.GetInt("page")
	if err != nil || page <= 0 {
		page = 1
	}
	pagenum, err := c.GetInt("pagenum")
	if err != nil || pagenum <= 0 {
		pagenum = 20
	}
	hnum := (page - 1) * pagenum
	tnum := hnum + pagenum

	status, err := c.GetInt("status")
	if err != nil {
		status = -1
	}

	gid, err := c.GetInt("gid")
	if err != nil || gid <= 0 {
		c.responseError(errors.New("group id error"), 1010)
		return
	}

	volumes := ops.OpsManager.GetVolume(uint64(gid))
	stores := global.STORES

	var rpVolumes respVolumebyId
	for _, v := range volumes {
		var statusStatic int
		for _, value := range v.Status {
			if value == global.Statusfull || value == global.Statuscanread {
				statusStatic += 1
			}
			if value == global.Statusfail || value == global.Statusrecover {
				statusStatic += 0
			}
			if value == global.Statushealth {
				statusStatic += 2
			}
		}

		rpVolume := new(respVolume)
		rpVolume.Id = v.Id
		if statusStatic == 0 {
			rpVolume.Status = global.Statusfail
		} else if statusStatic == 2*len(v.Status) {
			rpVolume.Status = global.Statushealth
		} else {
			rpVolume.Status = global.Statuscanread
		}

		//status filter
		if (status >= 0) && (rpVolume.Status != status) {
			continue
		}

		rpVolume.FreeSpace, isFull = unitExchange(v.FreeSpace * 8)
		if isFull && rpVolume.Status == global.Statushealth {
			//rpVolume.Status = global.Statuscanread
		}
		rpVolume.DelNums = uint64(v.Delnums)

		for _, vo := range v.StoreIds {
			rpVolume.StoreIps = append(rpVolume.StoreIps, stores[vo].Ip)
		}
		for _, vo := range v.Badstoreids {
			rpVolume.BadStoreIps = append(rpVolume.BadStoreIps, stores[vo].Ip)
		}

		rpVolumes = append(rpVolumes, rpVolume)
	}

	sort.Sort(rpVolumes)
	var respM map[string]interface{} = make(map[string]interface{})
	respM["amount"] = len(rpVolumes)
	if len(rpVolumes) <= hnum {
		c.responseError(errors.New("no record"), 1010)
		return
	}

	if len(rpVolumes) >= tnum {
		respM["data"] = rpVolumes[hnum:tnum]
	} else {
		respM["data"] = rpVolumes[hnum:]
	}

	c.responseOk(respM)
	return
}

func (c *ApiController) AddVolume() {
	c.pre()
	if !c.islogin() {
		c.responseError(errors.New("login first, please"), 1001)
		return
	}

	if !c.isAdmin() {
		c.responseError(errors.New("Non administrator, limit action"), 1010)
		return
	}

	var (
		err     error
		groupId int64
		n       int
	)

	if groupId, err = c.GetInt64("groupid"); err != nil {
		beego.Error(err)
		c.responseError(err, 1010)
		return
	}

	if n, err = c.GetInt("n"); err != nil {
		beego.Error(err)
		c.responseError(err, 1010)
		return
	}

	if err = ops.OpsManager.AddVolume(uint64(groupId), n); err != nil {
		beego.Error(err)
		c.responseError(err, 1010)
		return
	}

	//oplog
	c.addOpLog("add volume")

	c.responseOk("add ok")
	return
}

func (c *ApiController) CompactVolume() {
	c.pre()
	if !c.islogin() {
		c.responseError(errors.New("login first, please"), 1001)
		return
	}

	if !c.isAdmin() {
		c.responseError(errors.New("Non administrator, limit action"), 1010)
		return
	}

	vid, err := c.GetInt("vid")
	if err != nil {
		c.responseError(errors.New("volume id error"), 1010)
		return
	}

	global.Volume_lock.RLock()
	volume, ok := global.VOLUMES[uint64(vid)]
	global.Volume_lock.RUnlock()
	if !ok {
		c.responseError(errors.New("volume no exist"), 1010)
		return
	}

	for _, sid := range volume.StoreIds {
		global.Store_lock.RLock()
		store := global.STORES[sid]
		if err = ops.OpsManager.CompactVolume(store.Admin, uint64(vid)); err != nil {
			c.responseError(err, 1010)
			return
		}
		global.Store_lock.RUnlock()
	}

	//oplog
	c.addOpLog("compact volume")

	c.responseOk("compact ok")
	return
}

func (c *ApiController) RecoverVolume_del() {
	c.pre()
	if !c.islogin() {
		c.responseError(errors.New("login first, please"), 1001)
		return
	}
	if !c.isAdmin() {
		c.responseError(errors.New("Non administrator, limit action"), 1010)
		return
	}

	vid, err := c.GetInt64("vid")
	if err != nil {
		c.responseError(errors.New("volume id error"), 1010)
		return
	}

	global.Volume_lock.RLock()
	volume, ok := global.VOLUMES[uint64(vid)]
	global.Volume_lock.RUnlock()
	if !ok {
		c.responseError(errors.New("volume no exist"), 1010)
		return
	}

	global.Store_lock.RLock()
	stores := global.STORES
	global.Store_lock.RUnlock()

	var statusStatic int
	var readStoreId []string
	var badStoreId []string
	for storeId, value := range volume.Status {
		if value == global.Statusfull || value == global.Statuscanread {
			statusStatic += 1
			readStoreId = append(readStoreId, storeId)
		}
		if value == global.Statusfail {
			statusStatic += 0
			badStoreId = append(badStoreId, storeId)
		}
		if value == global.Statushealth {
			statusStatic += 2
			readStoreId = append(readStoreId, storeId)
		}
	}
	if statusStatic == 0 {
		c.responseError(errors.New("volume failed, can't recover"), 1010)
		return
	}
	if statusStatic == 2*len(volume.Status) {
		c.responseError(errors.New("volume health, needn't recover"), 1010)
		return
	}

	for _, sid := range badStoreId {
		store := stores[sid]
		if err = ops.OpsManager.RecoverVolume(stores[readStoreId[0]].Rebalance, store.Rack, store.Id, uint64(vid)); err != nil {
			c.responseError(err, 1010)
			return
		}
	}

	//oplog
	c.addOpLog("recover volume")

	c.responseOk("recover ok")
	return
}

func (c *ApiController) RecoverVolume() {
	c.pre()
	if !c.islogin() {
		c.responseError(errors.New("login first, please"), 1001)
		return
	}
	if !c.isAdmin() {
		c.responseError(errors.New("Non administrator, limit action"), 1010)
		return
	}

	vid, err := c.GetInt64("vid")
	if err != nil {
		c.responseError(errors.New("volume id error"), 1010)
		return
	}

	srcIP := c.GetString("srcip")
	destIP := c.GetString("destip")
	if srcIP == "" || destIP == "" {
		c.responseError(errors.New("srcip or destip is empty"), 1010)
		return
	}

	global.Volume_lock.RLock()
	volume, ok := global.VOLUMES[uint64(vid)]
	global.Volume_lock.RUnlock()
	if !ok {
		c.responseError(errors.New("volume no exist"), 1010)
		return
	}

	global.Store_lock.RLock()
	stores := global.STORES
	global.Store_lock.RUnlock()

	var statusStatic int
	for _, value := range volume.Status {
		if value == global.Statusfull || value == global.Statuscanread {
			statusStatic += 1
		}
		if value == global.Statusfail {
			statusStatic += 0
		}
		if value == global.Statushealth {
			statusStatic += 2
		}
	}
	if statusStatic == 0 {
		c.responseError(errors.New("volume failed, can't recover"), 1010)
		return
	}
	/* // reset store change store from fail to ok
	if statusStatic == 2*len(volume.Status) {
		c.responseError(errors.New("volume health, needn't recover"), 1010)
		return
	}
	*/

	var srcStore, destStore *types.Store

	for _, store := range stores {
		if store.Ip == srcIP {
			srcStore = store
		}
		if store.Ip == destIP {
			destStore = store
		}
	}
	if srcStore == nil || destStore == nil {
		c.responseError(errors.New("srcStore or destStore no exist"), 1010)
		return
	}

	if err = ops.OpsManager.RecoverVolume(srcStore.Rebalance, destStore.Rack, destStore.Id, uint64(vid)); err != nil {
		c.responseError(err, 1010)
		return
	}

	//oplog
	c.addOpLog("recover volume")

	c.responseOk("recover ok")
	return
}

func (c *ApiController) RecoverVolumeStore() {
	c.pre()
	if !c.islogin() {
		c.responseError(errors.New("login first, please"), 1001)
		return
	}
	if !c.isAdmin() {
		c.responseError(errors.New("Non administrator, limit action"), 1010)
		return
	}

	vid, err := c.GetInt64("vid")
	if err != nil {
		c.responseError(errors.New("volume id error"), 1010)
		return
	}
	DestStoreID := c.GetString("destsid")
	if DestStoreID == "" {
		c.responseError(errors.New("dest store id error"), 1010)
		return
	}

	global.Volume_lock.RLock()
	volume, ok := global.VOLUMES[uint64(vid)]
	global.Volume_lock.RUnlock()
	if !ok {
		c.responseError(errors.New("volume no exist"), 1010)
		return
	}

	global.Store_lock.RLock()
	stores := global.STORES
	global.Store_lock.RUnlock()

	var statusStatic int
	var readStoreId []string
	for storeId, value := range volume.Status {
		if value == global.Statusfull || value == global.Statuscanread {
			statusStatic += 1
			readStoreId = append(readStoreId, storeId)
		}
		if value == global.Statusfail {
			statusStatic += 0
		}
		if value == global.Statushealth {
			statusStatic += 2
			readStoreId = append(readStoreId, storeId)
		}
	}
	if statusStatic == 0 {
		c.responseError(errors.New("volume failed, can't recover"), 1010)
		return
	}
	/*if statusStatic == 2*len(volume.Status) {
		c.responseError(errors.New("volume health, needn't recover"), 1010)
		return
	}
	*/
	store := stores[DestStoreID]
	if err = ops.OpsManager.RecoverVolume(stores[readStoreId[0]].Rebalance, store.Rack, store.Id, uint64(vid)); err != nil {
		c.responseError(err, 1010)
		return
	}

	//oplog
	c.addOpLog("recover volume")

	c.responseOk("recover ok")
	return
}

func (c *ApiController) RecoverStatus() {
	c.pre()
	if !c.islogin() {
		c.responseError(errors.New("login first, please"), 1001)
		return
	}
	if !c.isAdmin() {
		c.responseError(errors.New("Non administrator, limit action"), 1010)
		return
	}

	var (
		items    []*ops.RecoveryItem
		respItem []*ops.RecoveryItem
		respM    map[string]interface{}
		err      error
		status   int
	)
	respM = make(map[string]interface{})

	page, errP := c.GetInt("page")
	if errP != nil || page <= 0 {
		page = 1
	}
	pagenum, errPn := c.GetInt("pagenum")
	if errPn != nil || pagenum <= 0 {
		pagenum = 10
	}
	hnum := (page - 1) * pagenum
	tnum := hnum + pagenum

	if items, err = ops.OpsManager.RecoveryStatus(); err != nil {
		c.responseError(errors.New("get Recovery status fail"), 1010)
		return
	}

	if status, err = c.GetInt("status"); err != nil {
		status = 6
	}

	sort.Sort(ops.RecoveryItems(items))
	switch status {
	case 6:
		respItem = items
	case ops.Recovering:
		for _, vo := range items {
			if vo.ReStatus == ops.Recovering {
				respItem = append(respItem, vo)
			}
		}
	case ops.Recover_ok:
		for _, vo := range items {
			if vo.ReStatus == ops.Recover_ok {
				respItem = append(respItem, vo)
			}
		}
	case ops.Recover_failed:
		for _, vo := range items {
			if vo.ReStatus == ops.Recover_failed {
				respItem = append(respItem, vo)
			}
		}
	default:
		c.responseError(errors.New("error status"), 1010)
		return
	}

	if len(respItem) >= tnum {
		respM["recoverdata"] = respItem[hnum:tnum]
	} else {
		respM["recoverdata"] = respItem[hnum:]
	}

	respM["recovernum"] = len(respItem)
	c.responseOk(respM)
}

/***********************
	oplog
***********************/
func (c *ApiController) GetOpLog() {
	c.pre()
	if !c.islogin() {
		c.responseError(errors.New("login first, please"), 1001)
		return
	}

	page, err := c.GetInt("page")
	if err != nil || page <= 0 {
		page = 1
	}
	pagenum, err := c.GetInt("pagenum")
	if err != nil || pagenum <= 0 {
		pagenum = 20
	}

	opls, err := oplog.OpLogs(int32(page), int32(pagenum))
	if err != nil {
		c.responseError(errors.New("get operation log error"), 1010)
		return
	}

	respMs := make([]map[string]interface{}, 0)
	for _, opl := range opls {
		respM := make(map[string]interface{})
		users, err := user.UsersbyCon(map[string]string{"id": strconv.Itoa(int(opl.OpUId))})
		if err != nil || len(users) <= 0 {
			c.responseError(errors.New("get user error"), 1010)
			return
		}
		respM["acount"] = users[0].Acount
		respM["datetime"] = opl.OpTime
		respM["detail"] = opl.OpDetail

		respMs = append(respMs, respM)
	}

	c.responseOk(respMs)
}

/***********************
	almrec
***********************/
func (c *ApiController) GetAlmRec() {
	c.pre()
	if !c.islogin() {
		c.responseError(errors.New("login first, please"), 1001)
		return
	}

	page, err := c.GetInt("page")
	if err != nil || page <= 0 {
		page = 1
	}
	pagenum, err := c.GetInt("pagenum")
	if err != nil || pagenum <= 0 {
		pagenum = 20
	}

	almrecs, err := almrec.Almrecs(int32(page), int32(pagenum))
	if err != nil {
		c.responseError(errors.New("get alarm record error"), 1010)
		return
	}

	respMs := make([]map[string]interface{}, 0)
	//var acounts string
	for _, almrec := range almrecs {
		respM := make(map[string]interface{})
		respM["detail"] = almrec.AlarmDetail
		respM["time"] = almrec.CreateTime
		recvuid := almrec.ReceiveUID
		//uids := strings.Split(recvuid, "-")
		/*
			for _, uid := range uids {
				users, err := user.UsersbyCon(map[string]string{"id": uid})
				if err != nil || len(users) <= 0 {
					c.responseError(errors.New("get user error"), 1010)
					return
				}
				acounts += users[0].Acount + ";"
			}
		*/
		respM["acounts"] = recvuid

		respMs = append(respMs, respM)
	}

	c.responseOk(respMs)
}

/**********************
	user
**********************/
func (c *ApiController) GetUser() {
	c.pre()
	if !c.islogin() {
		c.responseError(errors.New("login first, please"), 1001)
		return
	}

	page, err := c.GetInt32("page")
	pageNum, err := c.GetInt32("pagenum")
	if err != nil || page <= 0 || pageNum <= 0 {
		page = 1
		pageNum = 99999
	}

	users, err := user.Users(page, pageNum)
	if err != nil {
		c.responseError(errors.New("users get error"), 1010)
		return
	}

	c.responseOk(users)
	return
}

func (c *ApiController) AddUser() {
	c.pre()
	if !c.islogin() {
		c.responseError(errors.New("login first, please"), 1001)
		return
	}

	if !c.isAdmin() {
		c.responseError(errors.New("limit operation"), 1010)
		return
	}

	acount := c.GetString("username")
	if len(acount) < 5 {
		c.responseError(errors.New("username too short"), 1010)
		return
	}
	passwd1 := c.GetString("passwd1")
	passwd2 := c.GetString("passwd2")
	if len(passwd1) < 6 || len(passwd2) < 6 || passwd1 != passwd2 {
		c.responseError(errors.New("passwd error"), 1010)
		return
	}
	passwd := c.encPasswd(passwd1, acount)

	name := c.GetString("name")
	if len(name) < 5 {
		c.responseError(errors.New("name too short"), 1010)
		return
	}
	role, err := c.GetInt32("role")
	if err != nil || (role != global.ROLE_ADMIN && role != global.ROLE_USER) {
		c.responseError(errors.New("role error"), 1010)
		return
	}
	alarm, err := c.GetInt32("alarm")
	if err != nil || (alarm != global.RECEIVE_ALARM && alarm != global.NRECEIVE_ALARM) {
		c.responseError(errors.New("alarm error"), 1010)
		return
	}
	mail := c.GetString("mail")
	if len(mail) < 10 {
		c.responseError(errors.New("mail error"), 1010)
		return
	}
	phone := c.GetString("phone")
	if phone != "" && len(phone) != 11 {
		c.responseError(errors.New("phone error"), 1010)
		return
	}
	qq := c.GetString("qq")
	if qq != "" && len(qq) < 5 {
		c.responseError(errors.New("qq error"), 1010)
		return
	}
	remark := c.GetString("remark")
	if remark != "" && len(remark) < 5 {
		c.responseError(errors.New("remark too short"), 1010)
	}

	userData := &user.User{Acount: acount, Password: passwd, Name: name, Role: role, Stat: 1,
		IsAlarm: alarm, Mail: mail, Phone: phone, QQ: qq, Remark: remark,
		LastLogin:  time.Now().Format("2006-01-02 15:04:05"),
		CreateTime: time.Now().Format("2006-01-02 15:04:05")}
	uid, err := user.Add(userData)
	if err != nil {
		c.responseError(errors.New("user add error, acount error"), 1010)
	}

	//oplog
	c.addOpLog("add user")

	c.responseOk(uid)
	return
}

func (c *ApiController) DelUser() {
	c.pre()
	if !c.islogin() {
		c.responseError(errors.New("login first, please"), 1001)
		return
	}

	if !c.isAdmin() {
		c.responseError(errors.New("limit operation"), 1010)
		return
	}

	uid, err := c.GetInt32("uid")
	if err != nil || uid <= 0 {
		c.responseError(errors.New("uid error"), 1010)
		return
	}

	cuid := c.cUId()
	if cuid == 0 || cuid == uid {
		c.responseError(errors.New("uid error"), 1010)
		return
	}

	ra, err := user.DeletebyID(uid)
	if err != nil || ra == false {
		c.responseError(errors.New("user delete error"), 1010)
		return
	}

	//oplog
	c.addOpLog("del user")

	c.responseOk("user delete ok")
	return
}

func (c *ApiController) ActivateUser() {
	c.pre()
	if !c.islogin() {
		c.responseError(errors.New("login first, please"), 1001)
		return
	}

	if !c.isAdmin() {
		c.responseError(errors.New("limit operation"), 1010)
		return
	}

	var isActivate string
	activate := c.GetString("activate")
	switch activate {
	case "activate":
		isActivate = "1"
	default:
		isActivate = "2"
	}

	uid, err := c.GetInt32("uid")
	if err != nil {
		c.responseError(errors.New("uid error"), 1010)
	}

	cuid := c.cUId()
	if cuid == 0 || cuid == uid {
		c.responseError(errors.New("uid error"), 1010)
		return
	}

	ra, err := user.UpdatebyID(map[string]string{"stat": isActivate}, uid)
	if err != nil || ra == false {
		c.responseError(errors.New("user stat modify error"), 1010)
		return
	}

	//oplog
	c.addOpLog("activate user")

	c.responseOk("user stat modify ok")
	return
}

func (c *ApiController) ResetUserPasswd() {
	c.pre()
	if !c.islogin() {
		c.responseError(errors.New("login first, please"), 1001)
		return
	}

	if !c.isAdmin() {
		c.responseError(errors.New("limit operation"), 1010)
		return
	}

	passwd1 := c.GetString("passwd1")
	passwd2 := c.GetString("passwd2")
	if passwd1 != passwd2 || len(passwd1) < 6 {
		c.responseError(errors.New("passwd error"), 1010)
		return
	}
	acount := c.GetString("username")
	if len(acount) < 5 {
		c.responseError(errors.New("acount error"), 1010)
		return
	}
	passwd := c.encPasswd(passwd1, acount)

	uid, err := c.GetInt32("uid")
	if err != nil || uid <= 0 {
		c.responseError(errors.New("uid error"), 1010)
		return
	}

	cuid := c.cUId()
	if cuid == 0 || cuid == uid {
		c.responseError(errors.New("uid error"), 1010)
		return
	}

	ra, err := user.UpdatebyID(map[string]string{"password": passwd}, uid)
	if err != nil || ra == false {
		c.responseError(errors.New("update user error"), 1010)
		return
	}

	//oplog
	c.addOpLog("reset user password")

	c.responseOk("reset ok")
}

func (c *ApiController) ModifyPasswd() {
	c.pre()
	if !c.islogin() {
		c.responseError(errors.New("login first, please"), 1001)
		return
	}

	passwd1 := c.GetString("passwd1")
	passwd2 := c.GetString("passwd2")
	if passwd1 != passwd2 || len(passwd1) < 6 {
		c.responseError(errors.New("password error"), 1010)
		return
	}
	acount := c.GetString("username")
	if len(acount) < 5 {
		c.responseError(errors.New("acount error"), 1010)
		return
	}
	passwd := c.encPasswd(passwd1, acount)

	uid := c.cUId()
	if uid == 0 {
		c.responseError(errors.New("uid error, login again, please"), 1001)
		return
	}

	ra, err := user.UpdatebyID(map[string]string{"password": passwd}, uid)
	if err != nil || ra == false {
		c.responseError(errors.New("update password error"), 1010)
		return
	}

	c.responseOk("modify ok")
	return
}

func (c *ApiController) GetUserbyId() {
	c.pre()
	if !c.islogin() {
		c.responseError(errors.New("login first, please"), 1001)
		return
	}

	uid, err := c.GetInt32("uid")
	if err != nil || uid <= 0 {
		c.responseError(errors.New("uid error"), 1010)
		return
	}

	users, err := user.UsersbyCon(map[string]string{"id": strconv.Itoa(int(uid))})
	if err != nil || len(users) != 1 {
		c.responseError(errors.New("get user error"), 1010)
		return
	}

	c.responseOk(users[0])
	return
}

func (c *ApiController) ResetUser() {
	c.pre()
	if !c.islogin() {
		c.responseError(errors.New("login first, please"), 1001)
		return
	}

	uid, err := c.GetInt32("uid")
	if err != nil || uid <= 0 {
		c.responseError(errors.New("uid error"), 1010)
		return
	}

	role, err := c.GetInt32("role")
	if err != nil || (role != global.ROLE_ADMIN && role != global.ROLE_USER) {
		c.responseError(errors.New("role error"), 1010)
		return
	}
	alarm, err := c.GetInt32("alarm")
	if err != nil || (alarm != global.RECEIVE_ALARM && alarm != global.NRECEIVE_ALARM) {
		c.responseError(errors.New("alarm error"), 1010)
		return
	}
	mail := c.GetString("mail")
	if len(mail) < 10 {
		c.responseError(errors.New("mail error"), 1010)
		return
	}
	phone := c.GetString("phone")
	if phone != "" && len(phone) != 11 {
		c.responseError(errors.New("phone error"), 1010)
		return
	}
	qq := c.GetString("qq")
	if qq != "" && len(qq) < 5 {
		c.responseError(errors.New("qq error"), 1010)
		return
	}
	remark := c.GetString("remark")
	if remark != "" && len(remark) < 5 {
		c.responseError(errors.New("remark too short"), 1010)
	}

	updateCon := map[string]string{"role": strconv.Itoa(int(role)),
		"is_alarm": strconv.Itoa(int(alarm)), "mail": mail,
		"phone": phone, "qq": qq, "remark": remark}

	ra, err := user.UpdatebyID(updateCon, uid)
	if err != nil || ra == false {
		c.responseError(errors.New("user update error"), 1010)
		return
	}

	//oplog
	c.addOpLog("reset user info")

	c.responseOk("update ok")
	return
}

func (c *ApiController) ModifyUser() {
	c.pre()
	if !c.islogin() {
		c.responseError(errors.New("login first, please"), 1001)
		return
	}

	uid := c.cUId()
	if uid == 0 {
		c.responseError(errors.New("login again, please"), 1001)
		return
	}

	mail := c.GetString("mail")
	if len(mail) < 10 {
		c.responseError(errors.New("mail error"), 1010)
		return
	}
	phone := c.GetString("phone")
	if phone != "" && len(phone) != 11 {
		c.responseError(errors.New("phone error"), 1010)
		return
	}
	qq := c.GetString("qq")
	if qq != "" && len(qq) < 5 {
		c.responseError(errors.New("qq error"), 1010)
		return
	}
	remark := c.GetString("remark")
	if remark != "" && len(remark) < 5 {
		c.responseError(errors.New("remark too short"), 1010)
	}

	updateCon := map[string]string{"mail": mail,
		"phone": phone, "qq": qq, "remark": remark}

	ra, err := user.UpdatebyID(updateCon, uid)
	if err != nil || ra == false {
		c.responseError(errors.New("user update error"), 1010)
		return
	}

	c.responseOk("update ok")
	return
}

/**********************
	rebalance
**********************/
const (
	move_ready   = 0
	moving       = 1
	move_ok      = 2 //src vol move ok ,new is not
	move_failed  = 3
	move_success = 4 // 4 is success

	rebalance_doing  = "doing"  //1
	rebalance_stop   = "stop"   //2
	rebalance_finish = "finish" //3
)

func (c *ApiController) RebalanceStart() {
	var (
		status          string
		err             error
		rebalanceServer string
		resp            *http.Response
	)
	c.pre()
	if !c.islogin() {
		c.responseError(errors.New("login first, please"), 1001)
		return
	}
	if !c.isAdmin() {
		c.responseError(errors.New("Non administrator, limit action"), 1010)
		return
	}

	if status, err = ops.OpsManager.RebalanceStatus(); err != nil {
		beego.Info(err)
		c.responseError(errors.New("get rebalance status error"), 1010)
		return
	}

	if status == rebalance_doing {
		c.responseError(errors.New("already start"), 1010)
		return
	}

	//oplog
	c.addOpLog("rebalance start")

	rebalanceServer = beego.AppConfig.String("RebalanceServer")
	//start
	startUrl := "http://" + rebalanceServer + "/startRebalance"
	if resp, err = http.Get(startUrl); err != nil {
		c.responseError(errors.New("start rebalance error"), 1010)
		return
	}
	if resp.StatusCode != http.StatusOK {
		c.responseError(errors.New("start rebalance error,statuscode:"+strconv.Itoa(resp.StatusCode)), 1010)
		return
	}

	c.responseOk("ok")
}

func (c *ApiController) RebalanceStatus() {
	var (
		compNum   int
		volNum    int
		status    string
		err       error
		respM     map[string]interface{}
		volStatus int
	)
	respM = make(map[string]interface{})
	c.pre()
	if !c.islogin() {
		c.responseError(errors.New("login first, please"), 1001)
		return
	}
	if !c.isAdmin() {
		c.responseError(errors.New("Non administrator, limit action"), 1010)
		return
	}

	if volStatus, err = c.GetInt("status"); err != nil {
		volStatus = 6
	}

	page, errP := c.GetInt("page")
	if errP != nil || page <= 0 {
		page = 1
	}
	pagenum, errPn := c.GetInt("pagenum")
	if errPn != nil || pagenum <= 0 {
		pagenum = 10
	}
	hnum := (page - 1) * pagenum
	tnum := hnum + pagenum

	if status, err = ops.OpsManager.RebalanceStatus(); err != nil {
		c.responseError(errors.New("get rebalance status error,status"), 1010)
		return
	}

	rebalanceVs, err1 := ops.OpsManager.RebalanceVids()
	if err1 != nil {
		c.responseError(errors.New("get rebalance data error"), 1010)
		return
	}
	volNum = len(rebalanceVs)
	//count completeness
	var isFinish bool
	for _, v := range rebalanceVs {
		isFinish = true
		for _, vo := range v.Stores {
			if vo.Status != move_success {
				isFinish = false
				break
			}
		}
		if isFinish {
			compNum++
		}
	}

	//volstatus filter
	tRebalanceVs := rebalanceVs
	tRebalanceVs = nil
	var tStatus bool
	switch volStatus {
	case 6:
		tRebalanceVs = rebalanceVs
	case 0:
		for _, vo := range rebalanceVs {
			for _, vol := range vo.Stores {
				if vol.Status == move_ready {
					tRebalanceVs = append(tRebalanceVs, vo)
					break
				}
			}
		}
	case 1:
		for _, vo := range rebalanceVs {
			for _, vol := range vo.Stores {
				if vol.Status == moving || vol.Status == move_ok {
					tRebalanceVs = append(tRebalanceVs, vo)
					break
				}
			}
		}
	case 4:
		for _, vo := range rebalanceVs {
			tStatus = true
			for _, vol := range vo.Stores {
				if vol.Status != move_success {
					tStatus = false
					break
				}
			}
			if tStatus {
				tRebalanceVs = append(tRebalanceVs, vo)
			}
		}
	case 3:
		for _, vo := range rebalanceVs {
			for _, vol := range vo.Stores {
				if vol.Status == move_failed {
					tRebalanceVs = append(tRebalanceVs, vo)
					break
				}
			}
		}
	default:
		c.responseError(errors.New("option choose error"), 1010)
		return
	}

	if len(tRebalanceVs) >= tnum {
		respM["reblncData"] = tRebalanceVs[hnum:tnum]
	} else {
		respM["reblncData"] = tRebalanceVs[hnum:]
	}
	if tRebalanceVs == nil {
		respM["reblncData"] = make([]string, 0)
	}

	respM["reblncNum"] = len(tRebalanceVs)
	if status == rebalance_doing {
		respM["reblncStatus"] = 1
	} else if status == rebalance_stop {
		respM["reblncStatus"] = 2
	} else {
		respM["reblncStatus"] = 3
	}

	if volNum == 0 {
		respM["compRate"] = 100
	} else {
		respM["compRate"] = int(float32(compNum) / float32(volNum) * 100)
	}
	c.responseOk(respM)
	return
}

func (c *ApiController) RebalanceFinish() {
	var (
		status          string
		err             error
		rebalanceServer string
		resp            *http.Response
	)
	c.pre()
	if !c.islogin() {
		c.responseError(errors.New("login first, please"), 1001)
		return
	}
	if !c.isAdmin() {
		c.responseError(errors.New("Non administrator, limit action"), 1010)
		return
	}

	if status, err = ops.OpsManager.RebalanceStatus(); err != nil {
		c.responseError(errors.New("get rebalance status error"), 1010)
		return
	}

	if status != rebalance_doing {
		c.responseError(errors.New("already stop"), 1010)
		return
	}

	//oplog
	c.addOpLog("rebalance stop")
	rebalanceServer = beego.AppConfig.String("RebalanceServer")
	//stop
	finishUrl := "http://" + rebalanceServer + "/stopRebalance"
	if resp, err = http.Get(finishUrl); err != nil {
		c.responseError(errors.New("stop rebalance error"), 1010)
		return
	}
	if resp.StatusCode != http.StatusOK {
		c.responseError(errors.New("stop rebalance error,statuscode:"+strconv.Itoa(resp.StatusCode)), 1010)
		return
	}

	c.responseOk("ok")
}

/**********************
	common
**********************/
func (c *ApiController) responseError(err error, statusCode int32) {
	res := make(map[string]interface{})
	res["success"] = false
	res["statuscode"] = statusCode
	res["msg"] = err.Error()
	c.Data["json"] = res
	c.ServeJSON()
}

func (c *ApiController) responseOk(data interface{}) {
	res := make(map[string]interface{})
	res["success"] = true
	res["data"] = data
	c.Data["json"] = res
	c.ServeJSON()
}

func (c *ApiController) pre() {
	c.cors()
}

func (c *ApiController) cors() {
	c.Ctx.Output.Header("Access-Control-Allow-Origin", "*")
	//c.Ctx.Output.Header("Access-Control-Allow-Headers", "Content-Type")
	//c.Ctx.Output.Header("content-type", "application/json")
}

func (c *ApiController) islogin() bool {
	const VALID_TIME = 24 * 3600
	token := c.GetString("token")
	if token == "" {
		return false
	}

	uidtime, ok := user.LoginUsers[token]
	if ok == false {
		return false
	}

	data := strings.Split(uidtime, "-")
	pre, err := strconv.Atoi(data[1])
	if err != nil {
		beego.Error(err)
		return false
	}

	if time.Now().Unix()-int64(pre) > VALID_TIME {
		return false
	}

	return true
}

func (c *ApiController) encPasswd(passwd, acount string) (passStr string) {
	passMd5 := md5.Sum([]byte(passwd + "-" + acount))
	passStr = hex.EncodeToString(passMd5[:])

	return
}

func (c *ApiController) isAdmin() bool {
	token := c.GetString("token")
	uidtime := user.LoginUsers[token]
	adminUid := strings.Split(uidtime, "-")[0]

	isExist, err := user.UserisExist(map[string]string{"id": adminUid,
		"role": strconv.Itoa(int(global.ROLE_ADMIN))})
	if err != nil || !isExist {
		return false
	}

	return true
}

//oplog
func (c *ApiController) addOpLog(detail string) {
	token := c.GetString("token")
	uidtime := user.LoginUsers[token]
	uidstr := strings.Split(uidtime, "-")[0]
	uid, _ := strconv.Atoi(uidstr)
	oplog.Add_OpLog(int32(uid), detail)
}

func (c *ApiController) cUId() (cuid int32) {
	token := c.GetString("token")
	uidtime := user.LoginUsers[token]
	uidstr := strings.Split(uidtime, "-")[0]
	uid, err := strconv.Atoi(uidstr)
	cuid = int32(uid)
	if err != nil {
		cuid = 0
		return
	}

	return
}

func (c *ApiController) httpget(url string) (ret string, err error) {
	if url == "" {
		err = errors.New("http get error: " + url)
		return
	}

	resp, err := http.Get(url)
	if err != nil {
		return
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return
	}
	ret = string(body)

	return
}

func calVolume(space string) (num uint64, err error) {
	space = strings.ToUpper(space)

	acount := space[0 : len(space)-1]
	unit := space[len(space)-1:]

	var tnum float64
	switch unit {
	case "M":
		num = 0
	case "G":
		tnum, err = strconv.ParseFloat(acount, 32)
		if err != nil {
			return
		}
		num = uint64(tnum) * 1024 * 1024 * 1024 / (global.VOLUME_SPACE + global.VOLUME_INDEX_SPACE)
	case "T":
		tnum, err = strconv.ParseFloat(acount, 32)
		if err != nil {
			return
		}
		num = uint64((tnum * float64(1024))) * 1024 * 1024 * 1024 /
			(global.VOLUME_SPACE + global.VOLUME_INDEX_SPACE)
	}

	return
}

//is slice repeat
func isSRept(s []string) (repeat bool) {
	var m map[string]int = make(map[string]int)
	for k, v := range s {
		if _, ok := m[v]; ok {
			return true
		}
		m[v] = k
	}

	return false
}

//group, volume capacity unit exchange
func unitExchange(space uint64) (strSpace string, isFull bool) {
	const Byte = 1
	const KB = 1024 * Byte
	const MB = 1024 * KB
	const GB = 1024 * MB
	const TB = 1024 * GB
	const PB = 1024 * TB

	maxRetainSpace, _ := beego.AppConfig.Int64("MaxRetainSpace")
	if space <= uint64(maxRetainSpace) {
		isFull = true
	}

	if space >= PB {
		s := float64(space) / float64(PB)
		strSpace = fmt.Sprintf("%.1f", s) + " PB"
		return
	}

	if space >= TB {
		s := float64(space) / float64(TB)
		strSpace = fmt.Sprintf("%.1f", s) + " TB"
		return
	}

	if space >= GB {
		s := float64(space) / float64(GB)
		strSpace = fmt.Sprintf("%.1f", s) + " GB"
		return
	}

	if space >= MB {
		s := float64(space) / float64(MB)
		strSpace = fmt.Sprintf("%.1f", s) + " MB"
		return
	}

	if space >= KB {
		s := float64(space) / float64(KB)
		strSpace = fmt.Sprintf("%.1f", s) + " KB"
		return
	}

	strSpace = strconv.Itoa(int(space)) + " Byte"
	return
}
