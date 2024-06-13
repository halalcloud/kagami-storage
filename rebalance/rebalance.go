package main

import (
	"bytes"
	"efs/libs/errors"
	"efs/libs/meta"
	log "efs/log/glog"
	"efs/rebalance/conf"
	myzk "efs/rebalance/zk"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"mime/multipart"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	itime "github.com/Terry-Mao/marmot/time"
)

const (
	retrySleep = time.Second * 1
)

const (
	_rebalanceApi     = "http://%s/movevolume"
	_storeAdminApi    = "http://%s/add_volume"
	_volumeMessageApi = "http://%s/getvolumeinfo"
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
	_rand     = rand.New(rand.NewSource(time.Now().UnixNano()))
)

// Rebalance
// id means store serverid; vid means volume id; gid means group id
type Rebalance struct {
	config *conf.Config
	zk     *myzk.Zookeeper
}

// NewRebalance
func NewRebalance(config *conf.Config) (r *Rebalance, err error) {
	r = &Rebalance{}
	r.config = config
	if r.zk, err = myzk.NewZookeeper(config); err != nil {
		return
	}
	return
}

func (r *Rebalance) DoGetEfsMeta() (err error) {
	if err = r.syncRocksMeta(); err != nil {
		log.Errorf("DoGetEfsMeta: syncRocksMeta failed")
		return
	}
	if err = r.syncVolumesMeta(); err != nil {
		log.Errorf("DoGetEfsMeta: syncVolumesMeta failed")
		return
	}

	if err = r.syncGroupsMeta(); err != nil {
		log.Errorf("DoGetEfsMeta: syncGroupsMeta failed")
		return
	}

	if err = r.buildWillRebalanceMeta(GroupSizeR1); err != nil {
		log.Errorf("DoGetEfsMeta: buildWillRebalanceMeta failed")
		return
	}
	if err = r.buildWillRebalanceMeta(GroupSizeR2); err != nil {
		log.Errorf("DoGetEfsMeta: buildWillRebalanceMeta failed")
		return
	}
	if err = r.buildWillRebalanceMeta(GroupSizeR3); err != nil {
		log.Errorf("DoGetEfsMeta: buildWillRebalanceMeta failed")
		return
	}
	return
}

func (r *Rebalance) syncGroupsMeta() (err error) {
	var (
		str            string
		groups, stores []string
	)
	if groups, err = r.zk.Groups(); err != nil {
		return
	}
	for _, str = range groups {
		var (
			volume_count     int
			freevolume_count int
			vsizeInfo        meta.GroupSizeInfo
		)
		if stores, err = r.zk.GroupStores(str); err != nil {
			return
		}
		for _, storeid := range stores {
			var (
				uri  string
				res  meta.VolInfoResponse
				addr string
			)
			addr = StoreStatApi[storeid]
			uri = fmt.Sprintf(_volumeMessageApi, addr)

			if err = Http("GET", uri, nil, nil, &res); err != nil {
				log.Errorf("syncGroupsMeta: called Http error(%v)", err)
				return
			}
			for _, v := range res.VolMes {
				StorePathVids[storeid+"_"+v.Dirpath] = v.Vids
				volume_count += len(v.Vids)
				freevolume_count += len(v.Freevids)
				StorePaths[storeid] = append(StorePaths[storeid], v.Dirpath)
			}
		}
		vc := volume_count / len(stores)
		fvc := freevolume_count / len(stores)
		if vc <= 0 && fvc <= 0 {
			//tandebug
			//			log.Errorf("syncGroupsMeta: get volinfo failed")
			//			err = errors.ErrInternal
			//	return
		}
		vsizeInfo.VolumeCount = vc
		vsizeInfo.FreeVolumeCount = fvc

		//AllVolumeCount,AllFreeVolumeCount have no use
		AllVolumeCount += vsizeInfo.VolumeCount
		AllFreeVolumeCount += vsizeInfo.FreeVolumeCount

		if len(stores) == 1 {
			GroupSizeR1[str] = vsizeInfo
		} else if len(stores) == 2 {
			GroupSizeR2[str] = vsizeInfo
		} else {
			GroupSizeR3[str] = vsizeInfo
		}

		GroupStores[str] = stores
	}
	if err = r.BuildGroupSize(GroupSizeR1); err != nil {
		log.Errorf("syncGroupsMeta: BuildGroupSize failed")
		return
	}
	if err = r.BuildGroupSize(GroupSizeR2); err != nil {
		log.Errorf("syncGroupsMeta: BuildGroupSize failed")
		return
	}
	if err = r.BuildGroupSize(GroupSizeR3); err != nil {
		log.Errorf("syncGroupsMeta: BuildGroupSize failed")
		return
	}
	return
}

func (r *Rebalance) syncVolumesMeta() (err error) {
	var (
		maxvidcount int
		nodes       []string
	)

	if nodes, err = r.zk.Volumes(); err != nil {
		log.Errorf("syncVolumesMeta: Volumes failed")
		return
	}

	for _, vid := range nodes {
		var tvid int
		if tvid, err = strconv.Atoi(vid); err != nil {
			log.Errorf("syncVolumesMeta: Atoi(%s) failed", vid)
			return
		}
		if maxvidcount < tvid {
			maxvidcount = tvid
		}
	}
	MaxVolumeID = maxvidcount
	return
}
func (r *Rebalance) syncRocksMeta() (err error) {
	var (
		storeMeta              *meta.Store
		rack, str, volume      string
		racks, stores, volumes []string
		volumeMes              string
		storedata              []byte
		volumedata             []byte
	)

	if racks, _, err = r.zk.WatchRacks(); err != nil {
		log.Errorf("syncRocksMeta: WatchRacks failed (%v)", err)
		return
	}

	for _, rack = range racks {
		if stores, err = r.zk.Stores(rack); err != nil {
			log.Errorf("syncRocksMeta: zk Stores failed (%v)", err)
			return
		}

		for _, str = range stores {
			if storedata, err = r.zk.Store(rack, str); err != nil {
				log.Errorf("syncRocksMeta: zk store get value failed (%v)", err)
				return
			}
			storeMeta = new(meta.Store)
			if err = json.Unmarshal(storedata, storeMeta); err != nil {
				log.Errorf("syncRocksMeta: json.Unmarshal() error(%v)", err)
				return
			}
			StoreRblApi[storeMeta.Id] = storeMeta.Rebalance
			StoreAdminApi[storeMeta.Id] = storeMeta.Admin
			StoreStatApi[storeMeta.Id] = storeMeta.Stat
			StoreRock[str] = rack
			if volumes, err = r.zk.StoreVolumes(rack, str); err != nil {
				log.Errorf("syncRocksMeta: StoreVolumes failed (%v)", err)
				return
			}

			for _, volume = range volumes {
				VolumeStore[volume] = append(VolumeStore[volume], str)
				if volumedata, err = r.zk.StoreVolume(rack, str, volume); err != nil {
					log.Errorf("syncRocksMeta: StoreVolume failed (%v)", err)
					return
				}
				volumeMes = string(volumedata)
				l := strings.SplitN(volumeMes, ",", -1)
				c := strings.LastIndex(l[0], "/")
				if c <= 0 {
					log.Error("syncRocksMeta: split dist path failed")
					err = errors.ErrInternal
					return
				}
				VidStoreRblDisk[volume+"_"+storeMeta.Id] = l[0][0:c]
			}
		}
	}
	return
}

func (r *Rebalance) BuildGroupSize(GroupSize map[string]meta.GroupSizeInfo) (err error) {
	var (
		gpop              map[string]meta.GroupSizeInfo
		gput              map[string]meta.GroupSizeInfo
		put_all_count     int
		pop_all_count     int
		putarry           []string
		g_agv_count       int
		j, allvolumecount int
	)

	gpop = make(map[string]meta.GroupSizeInfo)
	gput = make(map[string]meta.GroupSizeInfo)
	for _, info := range GroupSize {
		allvolumecount += info.VolumeCount
	}
	if len(GroupSize) == 0 {
		return
	}
	g_agv_count = allvolumecount / len(GroupSize)

	for gid, info := range GroupSize {
		if g_agv_count > info.VolumeCount && info.FreeVolumeCount >= (g_agv_count-info.VolumeCount) {
			info.PutCount = g_agv_count - info.VolumeCount
			GroupSize[gid] = info
			put_all_count += info.PutCount
			gput[gid] = info
		} else {
			putarry = append(putarry, gid)
			gpop[gid] = info
		}
	}

	//pop_all_count = AllVolumeCount - put_all_count
	//pop nums = put nums tanmodify
	pop_all_count = put_all_count

	for i := 0; i < pop_all_count; i++ {
		if j == len(putarry) {
			j = 0
		}
		p := GroupSize[putarry[j]]
		p.PopCount += 1
		GroupSize[putarry[j]] = p
		j++
	}

	return
}

func (r *Rebalance) buildWillRebalanceMeta(GroupSize map[string]meta.GroupSizeInfo) (err error) {
	var CMaxVolumeID = MaxVolumeID
	var putVolumeStores map[string][]string
	putVolumeStores = make(map[string][]string)
	for gid, sinfo := range GroupSize {
		if sinfo.PutCount > 0 {
			for i := 0; i < sinfo.PutCount; i++ {
				CMaxVolumeID += 1
				sids := GroupStores[gid]
				vid := strconv.Itoa(CMaxVolumeID)
				//		fmt.Println("====up=====", sids, "=====", vid)
				putVolumeStores[vid] = sids
			}
		}
	}
	for gid, sinfo := range GroupSize {
		var svi meta.VidValue
		svi.DestRockId = make(map[string]string)
		if sinfo.PopCount > 0 {
			var (
				pi int
				vi int
			)
			sid := GroupStores[gid][0]
			paths := StorePaths[sid]

			for i := 0; i < sinfo.PopCount; i++ {
				if pi == len(paths) {
					pi = 0
					vi++
				}
				vids := StorePathVids[sid+"_"+paths[pi]]
				vid := vids[vi]
				SrcVidStoreid[vid] = VolumeStore[vid]
				MaxVolumeID += 1
				SrcVidDestVid[vid] = strconv.Itoa(MaxVolumeID)

				svi.DestVid = SrcVidDestVid[vid]
				svi.DestStoreId = putVolumeStores[svi.DestVid]
				for _, sid := range svi.DestStoreId {
					svi.DestRockId[sid] = StoreRock[sid] //TODO coredump
				}
				SrcVidInfos[vid] = svi
				pi++
			}
		}
	}
	return
}

func (r *Rebalance) SendRebalanceToStore(vid string, storeid string, vinfo meta.VidValue, addr string) (err error) {
	var (
		params = url.Values{}
		uri    string
	)
	uri = fmt.Sprintf(_rebalanceApi, addr)
	params.Set("mvolume", vid)
	params.Set("deststore", storeid)
	params.Set("newvolume", vinfo.DestVid)
	params.Set("rack", vinfo.DestRockId[storeid])

	if err = Http("POST", uri, params, nil, nil); err != nil {
		log.Errorf("SendRebalanceToStore called Http error(%v)", err)
		return
	}

	return
}

func (r *Rebalance) GetRebalanceValueInfo(storeRblCount map[string]int,
	distRblCount map[string]int, MoveReadyVid map[string]meta.VidInfo,
	MoveingVid map[string]meta.VidInfo, MoveOkVid map[string]meta.VidInfo,
	MoveFailedVid map[string]meta.VidInfo, MovedVid map[string]meta.VidInfo) (err error) {
	var (
		vids  []string
		vinfo map[string]meta.VidInfo
	)

	vinfo = make(map[string]meta.VidInfo)
	if vids, err = r.zk.Rebalance(); err != nil {
		log.Errorf("GetRebalanceValueInfo: Rebalance failed (%v)", err)
		return
	}

	for _, vid := range vids {
		var (
			v         meta.VidInfo
			storeinfo meta.StoreInfo
		)
		v.Storesinfo = make(map[string]meta.StoreInfo)
		vdata, terr := r.zk.GetRebalanceVidValue(vid)
		if terr != nil {
			log.Errorf("GetRebalanceValueInfo: GetRebalanceVidValue failed (%v)", terr)
			return
		}
		terr = json.Unmarshal(vdata, &(v.Vidinfo))
		if terr != nil {
			log.Errorf("GetRebalanceValueInfo: json.Unmarshal failed (%s)", terr.Error())
			return
		}
		stores, terr := r.zk.RebalanceVid(vid)
		if terr != nil {
			log.Errorf("GetRebalanceValueInfo: RebalanceVid failed (%v)", terr)
			return
		}
		for _, store := range stores {
			sdata, terr := r.zk.GetRebalanceVidStoreValue(vid, store)
			if terr != nil {
				log.Errorf("GetRebalanceValueInfo: GetRebalanceVidStoreValue failed (%v)", terr)
				return
			}
			if terr := json.Unmarshal(sdata, &storeinfo); terr != nil {
				log.Errorf("GetRebalanceValueInfo: json.Unmarshal failed (%s)", terr.Error())
				return
			}
			v.Storesinfo[store] = storeinfo
		}
		vinfo[vid] = v
	}

	for vd, vi := range vinfo {
		var status int
		for _, si := range vi.Storesinfo {
			switch si.MoveStatus {
			case _rebalanceMoveReadyStatus:
				status = _rebalanceMoveReadyStatus
			case _rebalanceMoveingStatus:
				status = _rebalanceMoveingStatus
			case _rebalanceMoveOkStatus:
				status = _rebalanceMoveOkStatus
			case _rebalanceMoveFailedStatus:
				status = _rebalanceMoveFailedStatus
			case _rebalanceMovedStatus:
				status = _rebalanceMovedStatus
			}

			switch status {
			case _rebalanceMoveReadyStatus:
				MoveReadyVid[vd] = vinfo[vd]
			case _rebalanceMoveingStatus:
				MoveingVid[vd] = vinfo[vd]
			case _rebalanceMoveOkStatus:
				MoveOkVid[vd] = vinfo[vd]
			case _rebalanceMoveFailedStatus:
				MoveFailedVid[vd] = vinfo[vd]
			case _rebalanceMovedStatus:
				MovedVid[vd] = vinfo[vd]
			}
		}
	}

	for vd, vi := range vinfo {
		for sd, si := range vi.Storesinfo {
			if si.MoveStatus == _rebalanceMoveingStatus {
				storeRblCount[sd] += 1
				disk := VidStoreRblDisk[vd+"_"+sd]
				distRblCount[vd+"_"+sd+"_"+disk] += 1
			}
		}
	}
	return
}

func (r *Rebalance) AllotNewVolumeToOnLine() (err error) {
	for gid, sinfo := range GroupSizeR1 {
		addCount := sinfo.FreeVolumeCount - sinfo.PutCount
		if addCount <= 0 {
			continue
		}
		for i := 0; i < addCount; i++ {
			MaxVolumeID += 1
			vid := strconv.Itoa(MaxVolumeID)
			OnLineVidStoreids[vid] = GroupStores[gid]
		}
	}

	for gid, sinfo := range GroupSizeR2 {
		addCount := sinfo.FreeVolumeCount - sinfo.PutCount
		if addCount <= 0 {
			continue
		}
		for i := 0; i < addCount; i++ {
			MaxVolumeID += 1
			vid := strconv.Itoa(MaxVolumeID)
			OnLineVidStoreids[vid] = GroupStores[gid]
		}
	}

	for gid, sinfo := range GroupSizeR3 {
		addCount := sinfo.FreeVolumeCount - sinfo.PutCount
		if addCount <= 0 {
			continue
		}
		for i := 0; i < addCount; i++ {
			MaxVolumeID += 1
			vid := strconv.Itoa(MaxVolumeID)
			OnLineVidStoreids[vid] = GroupStores[gid]
		}
	}
	return
}

func (r *Rebalance) DoAddNewVolumeToOnLine() (err error) {

	for nvid, stores := range OnLineVidStoreids {
		if stores != nil {
			for _, sid := range stores {
				var (
					uri    string
					params = url.Values{}
				)
				addr := StoreAdminApi[sid]
				uri = fmt.Sprintf(_storeAdminApi, addr)
				params.Set("vid", nvid)
				if err = Http("POST", uri, params, nil, nil); err != nil {
					log.Errorf("DoAddNewVolumeToOnLine: called Http error(%v)", err)
					return
				}
				if err = r.zk.AddRebalancedVolume(nvid, sid); err != nil {
					log.Errorf("DoAddNewVolumeToOnLine: AddRebalancedVolume volumeid(%s) storeid(%s) failed", nvid, sid)
					return
				}
			}
		}
	}
	return
}

func (r *Rebalance) DoSetRebalanceMeta() (err error) {

	if err = r.CreateNodeToRebalance(); err != nil {
		log.Errorf("DoSetRebalanceMeta: CreateNodeToRebalance failed")
		return
	}
	if err = r.SetRebalanceVidValue(); err != nil {
		log.Errorf("DoSetRebalanceMeta: SetRebalanceVidValue failed")
		return
	}

	return
}

func (r *Rebalance) CreateNodeToRebalance() (err error) {

	for vid, _ := range SrcVidStoreid {
		if err = r.zk.CreateRebalanceVid(vid); err != nil {
			log.Errorf("CreateNodeToRebalance: CreateRebalanceVid (%s) failed", vid)
			return
		}
	}

	for svid, sids := range SrcVidStoreid {
		for _, sid := range sids {
			var (
				data  []byte
				sinfo meta.StoreInfo
			)
			if err = r.zk.CreateRebalanceVidStore(svid, sid); err != nil {
				log.Errorf("CreateNodeToRebalance: CreateRebalanceVidStore svid(%s) sid(%s) failed", svid, sid)
				return
			}
			if data, err = json.Marshal(sinfo); err != nil {
				log.Errorf("CreateNodeToRebalance: json.Marshal failed err (%s)", err.Error())
				return
			}
			if err = r.zk.SetRebalanceVidStoreValue(svid, sid, data); err != nil {
				log.Errorf("CreateNodeToRebalance: SetRebalanceVidStoreValue svid(%s) sid(%s) failed", svid, sid)
				return
			}
		}
	}
	return
}

func (r *Rebalance) SetRebalanceVidValue() (err error) {

	for svid, info := range SrcVidInfos {
		var (
			data []byte
		)
		if data, err = json.Marshal(info); err != nil {
			log.Errorf("SetRebalanceVidValue: json.Marshal failed err (%s)", err.Error())
			return
		}
		if err = r.zk.SetRebalanceVidValue(svid, data); err != nil {
			log.Errorf("SetRebalanceVidValue: SetRebalanceVidValue svid(%s) failed", svid)
			return
		}
	}
	return
}

func (r *Rebalance) DoSetVidStoreRebalanceStatus(vid, storeid string, vinfo meta.StoreInfo, status int) (err error) {
	var (
		data []byte
	)
	vinfo.MoveStatus = status
	if data, err = json.Marshal(vinfo); err != nil {
		log.Errorf("DoSetVidStoreRebalanceStatus: json.Marshal failed err (%s)", err.Error())
		return
	}

	err = r.zk.SetVidStoreRebalanceStatus(vid, storeid, data)
	return
}

func (r *Rebalance) GetRebalanceStatus() (state string, err error) {
	state, err = r.zk.GetRebalanceStatusValue()
	return
}

func (r *Rebalance) SetRebalanceStatus(state string) (err error) {
	err = r.zk.SetRebalanceStatusValue(state)
	return
}

func (r *Rebalance) SetDispatcherModle(modle string) (err error) {
	err = r.zk.SetDispatcherModleValue(modle)
	return
}

func (r *Rebalance) CleanRebalanceChildren() (err error) {
	err = r.zk.CleanRebalanceChildrenNodes()
	return
}

func (r *Rebalance) DoAddRebalancedVolume(vid string, storeid string) (err error) {
	err = r.zk.AddRebalancedVolume(vid, storeid)
	return
}

// Http params
func Http(method, uri string, params url.Values, buf []byte, res interface{}) (err error) {
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
	} else {
		ru = uri
	}
	fmt.Println("url", ru)
	if method == "GET" {
		if req, err = http.NewRequest("GET", ru, nil); err != nil {
			return
		}
	} else {
		if buf == nil {
			if req, err = http.NewRequest("POST", ru, strings.NewReader(enc)); err != nil {
				return
			}
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
		err = errors.ErrInternal
		return
	}
	if res == nil {
		return
	}
	if body, err = ioutil.ReadAll(resp.Body); err != nil {
		log.Errorf("ioutil.ReadAll(%s) uri(%s) error(%v)", body, ru, err)
		return
	}
	if err = json.Unmarshal(body, res); err != nil {
		log.Errorf("json.Unmarshal(%s) uri(%s) error(%v)", body, ru, err)
	}
	return
}
