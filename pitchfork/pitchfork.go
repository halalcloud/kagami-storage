package main

import (
	"encoding/json"
	"kagamistoreage/libs/meta"
	"kagamistoreage/pitchfork/conf"
	myzk "kagamistoreage/pitchfork/zk"
	"net/http"
	"sort"
	"time"

	log "kagamistoreage/log/glog"

	"fmt"

	"github.com/samuel/go-zookeeper/zk"
)

const (
	_retrySleep = time.Second * 1
	_retryCount = 3
)

type Pitchfork struct {
	Id      string
	config  *conf.Config
	zk      *myzk.Zookeeper
	Ischeck bool
}

type Pgroup struct {
	Gid    string
	Stores []*meta.Store
}

type Pgrouplist []*Pgroup

func (sl Pgrouplist) Len() int {
	return len(sl)
}

func (sl Pgrouplist) Less(i, j int) bool {
	return sl[i].Gid < sl[j].Gid
}

func (sl Pgrouplist) Swap(i, j int) {
	sl[i], sl[j] = sl[j], sl[i]
}

// NewPitchfork
func NewPitchfork(config *conf.Config) (p *Pitchfork, err error) {
	var id string
	p = &Pitchfork{}
	p.config = config
	if p.zk, err = myzk.NewZookeeper(config); err != nil {
		log.Errorf("NewZookeeper() failed, Quit now")
		return
	}
	if id, err = p.init(); err != nil {
		log.Errorf("NewPitchfork failed error(%v)", err)
		return
	}
	p.Id = id
	return
}

// init register temporary pitchfork node in the zookeeper.
func (p *Pitchfork) init() (node string, err error) {
	node, err = p.zk.NewNode(p.config.Zookeeper.PitchforkRoot)
	return
}

// watchPitchforks get all the pitchfork nodes and set up the watcher in the zookeeper.
func (p *Pitchfork) watch() (res []string, ev <-chan zk.Event, err error) {
	if res, ev, err = p.zk.WatchPitchforks(); err == nil {
		sort.Strings(res)
	}
	return
}

// watchStores get all the store nodes and set up the watcher in the zookeeper.
func (p *Pitchfork) watchStores() (gres []*Pgroup, ev, gev <-chan zk.Event, err error) {
	var (
		rack, store, group             string
		racks, stores, groups, gstores []string
		data                           []byte
		storeMeta                      *meta.Store
		res                            map[string]*meta.Store
		pgroup                         *Pgroup
	)
	if racks, ev, err = p.zk.WatchRacks(); err != nil {
		log.Errorf("zk.WatchGetStore() error(%v)", err)
		return
	}
	res = make(map[string]*meta.Store)
	for _, rack = range racks {
		if stores, err = p.zk.Stores(rack); err != nil {
			return
		}
		for _, store = range stores {
			if data, err = p.zk.Store(rack, store); err != nil {
				return
			}
			storeMeta = new(meta.Store)
			if err = json.Unmarshal(data, storeMeta); err != nil {
				log.Errorf("json.Unmarshal() error(%v)", err)
				return
			}
			res[store] = storeMeta
		}
	}
	if groups, gev, err = p.zk.WatchGroups(); err != nil {
		log.Errorf("zk.watchgroups() error(%v)", err)
		return
	}
	for _, group = range groups {
		if gstores, err = p.zk.GroupStores(group); err != nil {
			return
		}
		pgroup = new(Pgroup)
		pgroup.Gid = group
		for _, store = range gstores {
			pgroup.Stores = append(pgroup.Stores, res[store])
		}
		gres = append(gres, pgroup)
	}

	sort.Sort(Pgrouplist(gres))
	return
}

func (p *Pitchfork) Checkdisk() {
	var (
		err      error
		serveMux = http.NewServeMux()
		server   = &http.Server{
			Addr:    p.config.HttpAddr,
			Handler: serveMux,
			// TODO read/write timeout
		}
	)
	serveMux.HandleFunc("/checkvolume", p.check)
	if err = server.ListenAndServe(); err != nil {
		log.Errorf("service check on failed %v", err)
		return
	}
	return
}

func (p *Pitchfork) check(wr http.ResponseWriter, r *http.Request) {

	go p.docheck()
}

func (p *Pitchfork) docheck() {
	var (
		//	stores     []*meta.Store
		gres       []*Pgroup
		pitchforks []string
		//sev, gev   <-chan zk.Event
		//pev        <-chan zk.Event
		//store      *meta.Store
		group *Pgroup
		err   error
	)

	if gres, _, _, err = p.watchStores(); err != nil {
		log.Errorf("watchGetStores() called error(%v)", err)
		return
	}
	if pitchforks, _, err = p.watch(); err != nil {
		log.Errorf("WatchGetPitchforks() called error(%v)", err)
		return
	}
	if gres = p.divide(pitchforks, gres); err != nil || len(gres) == 0 {
		log.Errorf("divide pithfork failed %v", err)
		return
	}

	for _, group = range gres {
		go p.checkNeedles(group)
	}

}

// Probe main flow of pitchfork server.
func (p *Pitchfork) Probe() {
	var (
		//	stores     []*meta.Store
		gres       []*Pgroup
		pitchforks []string
		sev, gev   <-chan zk.Event
		pev        <-chan zk.Event
		stop       chan struct{}
		//store      *meta.Store
		group *Pgroup
		err   error
	)
	for {
		if gres, sev, gev, err = p.watchStores(); err != nil {
			log.Errorf("watchGetStores() called error(%v)", err)
			time.Sleep(_retrySleep)
			continue
		}
		if pitchforks, pev, err = p.watch(); err != nil {
			log.Errorf("WatchGetPitchforks() called error(%v)", err)
			time.Sleep(_retrySleep)
			continue
		}
		if gres = p.divide(pitchforks, gres); err != nil || len(gres) == 0 {
			time.Sleep(_retrySleep)
			continue
		}
		stop = make(chan struct{})
		for _, group = range gres {
			go p.checkHealth(group, stop)
			//go p.checkNeedles(group, stop)
			go p.checkVolumes(group, stop)
		}
		select {
		case <-sev:
			log.Infof("store nodes change, rebalance")
		case <-gev:
			log.Infof("group change, rebalance")
		case <-pev:
			log.Infof("pitchfork nodes change, rebalance")
		case <-time.After(p.config.Store.RackCheckInterval.Duration):
			log.Infof("pitchfork poll zk")
		}
		close(stop)
	}

}

// divide a set of stores between a set of pitchforks.
func (p *Pitchfork) divide(pitchforks []string, gres []*Pgroup) (res []*Pgroup) {
	var (
		n, m        int
		ss, ps      int
		first, last int
		node        string
		group       *Pgroup
		sm          = make(map[string][]*Pgroup)
	)
	ss = len(gres)
	ps = len(pitchforks)
	if ss == 0 || ps == 0 || ss < ps {
		return nil
	}
	n = ss / ps
	m = ss % ps
	first = 0
	for _, node = range pitchforks {
		last = first + n
		if m > 0 {
			// let front node add one more
			last++
			m--
		}
		if last > ss {
			last = ss
		}
		for _, group = range gres[first:last] {
			sm[node] = append(sm[node], group)
		}
		first = last
	}
	return sm[p.Id]
}
func getvolumestatus(volume *meta.Volume) (vstatus int) {
	if volume.Block.LastErr != "" {
		log.Infof("get store block.lastErr:%s vid:%d", volume.Block.LastErr, volume.Id)
		//store.Status = meta.StoreStatusFail
		vstatus = meta.StoreStatusFail
	} else {
		vstatus = meta.StoreStatusHealth
	}

	if volume.Damage {
		log.Infof(" volume id %d is damage", volume.Id)
		//store.Status = meta.StoreStatusFail
		vstatus = meta.StoreStatusRecoverDoing
	}

	if vstatus == meta.StoreStatusHealth {
		if volume.Block.Full() || volume.Moving || volume.Compact {
			log.Infof("block: %s, offset: %d onlyready ", volume.Block.File, volume.Block.Offset)
			//store.Status = meta.StoreStatusRead
			vstatus = meta.StoreStatusRead
		}
	}
	return

}

func checkprobe(storeid string, volume *meta.Volume, stores []*meta.Store) (vstatus int, srcstoreid string) {
	var (
		other_store       []*meta.Store
		filekeys          []string
		store, this_store *meta.Store
		//err               error
	)
	vstatus = meta.StoreStatusHealth
	for _, store = range stores {
		if store.Id != storeid {
			other_store = append(other_store, store)
		} else {
			this_store = store
		}
	}
	for _, store = range other_store {
		filekeys = store.Getprobekey(volume.Id)
		fmt.Println(volume.Id, filekeys)
		if len(filekeys) == 0 {
			continue
		}
		//log.Errorf("src storeid %s deststoreid %s key %s", store.Id, this_store.Id, filekeys[0])
		//因为副本是强一致性，在磁盘没有损坏，其他副本的key 一定能在本节点能读到文件
		vstatus = this_store.Probekey(volume.Id, filekeys)
		if vstatus != meta.StoreStatusHealth {
			srcstoreid = store.Id
			break
		}
	}
	/*
		if len(other_store) == 0 {
			err = this_store.Head(volume.Id)
			if err != nil {
				vstatus = meta.StoreStatusFail
			}
		}
	*/
	return

}

func (p *Pitchfork) Probevolume(storeinfo map[string][]*meta.Volume, stores []*meta.Store) {
	var (
		storeid, srcstoreid string
		volumes             []*meta.Volume
		//store             *meta.Store
		volume  *meta.Volume
		vstatus int
		err     error
	)
	for storeid, volumes = range storeinfo {
		//	log.Errorf("storeid %s volume nums %d", storeid, len(volumes))
		//continue
		for _, volume = range volumes {
			//if vol
			//log.Errorf("num=%d", i)
			vstatus = getvolumestatus(volume)

			if vstatus != meta.StoreStatusRead && vstatus != meta.StoreStatusRecoverDoing {
				log.Errorf("store id %s volume id %d", storeid, volume.Id)
				vstatus, srcstoreid = checkprobe(storeid, volume, stores) //check next store vid
			} else {
				continue
			}

			if vstatus != meta.StoreStatusHealth {
				log.Errorf("vid %d storeid %s failed", volume.Id, storeid)
			} else {
				//log.Errorf("vid %d storeid %s helth======", volume.Id, storeid)
			}
			if vstatus == meta.StoreStatusRecover {
				if err = p.zk.SetVolumeRecoverState(volume.Id, storeid, srcstoreid); err != nil {
					log.Errorf("zk.SetVolumeRecoverState() error(%v)", err)
				}
			}

		}
	}

}

func (p *Pitchfork) checkNeedles(group *Pgroup) (err error) {
	var (
		i         int
		storeinfo map[string][]*meta.Volume
		store     *meta.Store
		volumes   []*meta.Volume
	)
	log.Infof("check needle health job group id %s start", group.Gid)

	storeinfo = make(map[string][]*meta.Volume)
	for _, store = range group.Stores {
		store.Status = meta.StoreStatusHealth
		for i = 0; i < _retryCount; i++ {
			if volumes, err = store.Info(); err == nil {
				break
			}
			time.Sleep(_retrySleep)
		}
		if err != nil {
			log.Errorf("store %s is not connect", store.Id)
			store.Status = meta.StoreStatusFail
		} else {
			storeinfo[store.Id] = volumes
		}
		if err = p.zk.SetStore(store); err != nil {
			log.Errorf("update store zk status failed, retry")
			continue
		}
	}

	p.Probevolume(storeinfo, group.Stores)
	return

}
func (p *Pitchfork) CheckvolumeStat(storeinfo map[string][]*meta.Volume, stores []*meta.Store) {
	var (
		storeid string
		volumes []*meta.Volume
		//store             *meta.Store
		volume  *meta.Volume
		vstatus int
		err     error
	)
	for storeid, volumes = range storeinfo {
		//log.Errorf("storeid %s volume nums %d", storeid, len(volumes))
		//continue
		for _, volume = range volumes {
			//if vol
			//log.Errorf("num=%d", i)
			vstatus = getvolumestatus(volume)

			if vstatus == meta.StoreStatusRecoverDoing {
				log.Errorf("storeid %s volume id %d is doing recover", storeid, volume.Id)
				vstatus = meta.StoreStatusFail
			}
			if vstatus != meta.StoreStatusHealth {
				log.Errorf("vid %d storeid %s failed", volume.Id, storeid)
			}
			if err = p.zk.SetVolumeState(volume, storeid, vstatus); err != nil {
				log.Errorf("zk.SetVolumeState() error(%v)", err)
			}
		}
	}

}
func (p *Pitchfork) checkVolumes(group *Pgroup, stop chan struct{}) (err error) {
	var (
		i         int
		storeinfo map[string][]*meta.Volume
		store     *meta.Store
		volumes   []*meta.Volume
	)
	log.Infof("check needle health job group id %s start", group.Gid)
	for {
		storeinfo = make(map[string][]*meta.Volume)
		for _, store = range group.Stores {
			store.Status = meta.StoreStatusHealth
			for i = 0; i < _retryCount; i++ {
				if volumes, err = store.Info(); err == nil {
					break
				}
				time.Sleep(_retrySleep)
			}
			if err != nil {
				log.Errorf("store %s is not connect", store.Id)
				store.Status = meta.StoreStatusFail
			} else {
				storeinfo[store.Id] = volumes
			}
			if err = p.zk.SetStore(store); err != nil {
				log.Errorf("update store zk status failed, retry")
				continue
			}
		}

		p.CheckvolumeStat(storeinfo, group.Stores)
		//return
		select {
		case <-stop:
			log.Infof("check_health job stop")
			return
		case <-time.After(p.config.Store.NeedleCheckInterval.Duration):
			break
		}

	}
}

func (p *Pitchfork) checkHealth(group *Pgroup, stop chan struct{}) (err error) {
	var (
		store *meta.Store
	)
	log.Infof("check_health job start")
	for {
		for _, store = range group.Stores {
			err = store.Checkping()
			if err != nil {
				log.Errorf("store %s is not connect", store.Id)
				store.Status = meta.StoreStatusFail
			} else {
				store.Status = meta.StoreStatusHealth
			}

			if err = p.zk.SetStore(store); err != nil {
				log.Errorf("update store zk status failed, retry")

			}
		}

		select {
		case <-stop:
			log.Infof("check_health job stop")
			return
		case <-time.After(p.config.Store.StoreCheckInterval.Duration):
			break
		}
	}
}

/*
// checkHealth check the store health.
func (p *Pitchfork) checkHealth(group *Pgroup, stop chan struct{}) (err error) {
	var (
		status, vstatus, i int
		volume             *meta.Volume
		volumes            []*meta.Volume
		store              *meta.Store
	)
	log.Infof("check_health job start")
	for {
		select {
		case <-stop:
			log.Infof("check_health job stop")
			return
		case <-time.After(p.config.Store.StoreCheckInterval.Duration):
			break
		}
		//status = store.Status

		for _, store = range group.Stores {
			store.Status = meta.StoreStatusHealth
			for i = 0; i < _retryCount; i++ {
				if volumes, err = store.Info(); err == nil {
					break
				}
				time.Sleep(_retrySleep)
			}
			if err == nil {
				for _, volume = range volumes {
					if volume.Block.LastErr != "" {
						log.Infof("get store block.lastErr:%s host:%s", volume.Block.LastErr, store.Stat)
						//store.Status = meta.StoreStatusFail
						vstatus = meta.StoreStatusFail
					} else {
						vstatus = meta.StoreStatusHealth
					}
					if volume.Damage {
						log.Infof("store id %s volume id %d is damage", store.Id, volume.Id)
						//store.Status = meta.StoreStatusFail
						vstatus = meta.StoreStatusFail
					}

					if vstatus == meta.StoreStatusHealth {
						if volume.Block.Full() || volume.Moving || volume.Compact {
							log.Infof("block: %s, offset: %d onlyready ", volume.Block.File, volume.Block.Offset)
							//store.Status = meta.StoreStatusRead
							vstatus = meta.StoreStatusRead
						}
					}
					if err = p.zk.SetVolumeState(volume, store.Id, vstatus); err != nil {
						log.Errorf("zk.SetVolumeState() error(%v)", err)
					}
				}
			} else {
				log.Errorf("get store info failed, retry host:%s", store.Stat)
				store.Status = meta.StoreStatusFail
			}

			if err = p.zk.SetStore(store); err != nil {
				log.Errorf("update store zk status failed, retry")
				continue
			}

		}

	}
	return
}
*/
// checkNeedles check the store health.
func (p *Pitchfork) checkNeedles_del(store *meta.Store, stop chan struct{}) (err error) {
	var (
		status  int
		volume  *meta.Volume
		volumes []*meta.Volume
	)
	log.Infof("checkNeedles job start")
	for {
		select {
		case <-stop:
			log.Infof("checkNeedles job stop")
			return
		case <-time.After(p.config.Store.NeedleCheckInterval.Duration):
			break
		}
		status = store.Status
		if volumes, err = store.Info(); err != nil {
			log.Errorf("get store info failed, retry host:%s", store.Stat)
			store.Status = meta.StoreStatusFail
		} else {
			store.Status = meta.StoreStatusHealth
		}

		if status != store.Status {
			if err = p.zk.SetStore(store); err != nil {
				log.Errorf("update store zk status failed, retry")
				continue
			}
		}

		if store.Status == meta.StoreStatusFail {
			continue
		}

		for _, volume = range volumes {
			/*
				if err = volume.Block.LastErr; err != nil {
					break
				}
			*/

			if volume.Moving {
				continue
			}

			err = store.Head(volume.Id)

		}
		/*
			failed:
				if status != store.Status {
					if err = p.zk.SetStore(store); err != nil {
						log.Errorf("update store zk status failed, retry")
						continue
					}
				}
		*/
	}
}
