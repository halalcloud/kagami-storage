package main

import (
	"kagamistoreage/libs/errors"
	"kagamistoreage/libs/meta"
	"math/rand"
	"sort"
	"sync"
	"time"

	log "kagamistoreage/log/glog"
)

// Dispatcher
// get raw data and processed into memory for http reqs
type Dispatcher struct {
	gids          []int           // for write eg:  gid:1;2   gids: [1,1,2,2,2,2,2]
	gidRepl       map[int]int     // group replication numbers
	groupvolhelth map[int][]int32 //for write to helth volume gid:[vid]
	polinggid     int             //poling flag
	rand          *rand.Rand
	rlock         sync.Mutex
}

const (
	maxScore       = 1000
	nsToMs         = 1000000             // ns ->  us
	spaceBenchmark = meta.MaxBlockOffset // 1 volume
	//addDelayBenchmark = 100                 // 100ms   <100ms means no load, -Score==0
	//baseAddDelay      = 100                 // 1s score:   -(1000/baseAddDelay)*addDelayBenchmark == -1000
	//TODO  if group disk is more 30t,use 100ms mean load .less 30t so use 2s mean load
	addDelayBenchmark = 2000 //second
	baseAddDelay      = 10   //10 score/second
)

// NewDispatcher
func NewDispatcher() (d *Dispatcher) {
	d = new(Dispatcher)
	d.rand = rand.New(rand.NewSource(time.Now().UnixNano()))
	d.groupvolhelth = make(map[int][]int32)
	d.gidRepl = make(map[int]int)
	return
}

func (d *Dispatcher) VolumeCanWrite(status int) bool {
	return status == meta.StoreStatusWrite || status == meta.StoreStatusHealth
}

func (d *Dispatcher) VolumeCanRead(status int) bool {
	return status == meta.StoreStatusRead || status == meta.StoreStatusHealth
}

// Update when zk updates
func (d *Dispatcher) Update(group map[int][]string,
	store map[string]*meta.Store, volume map[int32]*meta.VolumeState,
	storeVolume map[string][]int32, volumestorestatus map[int32]map[string]int) (err error) {
	var (
		gid                        int
		gidRepl                    int
		i                          int
		vid                        int32
		gids                       []int
		gidsRepl                   map[int]int
		vids                       []int32
		sid                        string
		stores                     []string
		restSpace, minScore, score int
		totalAdd, totalAddDelay    uint64
		write, ok                  bool
		storeMeta                  *meta.Store
		volumeState                *meta.VolumeState
		storestatus                map[string]int
		flag                       bool
	)
	gids = []int{}
	gidsRepl = make(map[int]int)

	for gid, stores = range group {
		write = true
		gidRepl = len(stores)
		// check all stores can writeable by the group.
		for _, sid = range stores {
			if storeMeta, ok = store[sid]; !ok {
				log.Errorf("idStore cannot match store: %s", sid)
				return
			}
			if storeMeta == nil {
				log.Errorf("storeMeta is null, %s", sid)
				return
			}
			/*
				if !storeMeta.CanWrite() {
					write = false
					break
				}
			*/
			if storeMeta.IsFail() { //if store isnot shutdown or network is ok,it can write
				write = false
				log.Errorf("gid(%d)'s sid(%d) is failed", gid, sid)
				break
			}
		}
		if !write {
			continue
		}
		vids = []int32{}
		flag = false

		// calc score
		minScore = 0
		for _, sid = range stores {
			totalAdd, totalAddDelay, restSpace = 0, 0, 0

			// get all volumes by the store.
			for _, vid = range storeVolume[sid] {
				volumeState, ok = volume[vid]
				if volumeState == nil || !ok {
					log.Errorf("volumeState is nil or zk /volume have no this vid, %d", vid)
					continue
				}

				write = true // volume status is can write
				storestatus, ok = volumestorestatus[vid]
				if !ok {
					log.Errorf("volume store status have no this vid %d ", vid)
					continue
				}
				for _, status := range storestatus {
					if !d.VolumeCanWrite(status) {
						log.Infof("sid(%d)'s vid(%d) is failed", sid, vid)
						write = false
						break
					}
				}

				if !flag {
					if write {
						vids = append(vids, vid)
					}
				}
				totalAdd = totalAdd + volumeState.TotalWriteProcessed
				if write { //if volume is can write,freespace++
					restSpace = restSpace + int(volumeState.FreeSpace)
				}
				totalAddDelay = totalAddDelay + volumeState.TotalWriteDelay
			}
			flag = true
			score = d.calScore(int(totalAdd), int(totalAddDelay), restSpace)
			log.Infof("gid(%d) sid(%s) score(%d) tp(%d) td(%d) rs(%d)", gid, sid, score, totalAdd, totalAddDelay, restSpace)
			if score < minScore || minScore == 0 {
				minScore = score
			}
		}
		if len(vids) == 0 {
			continue
		}

		d.rlock.Lock()
		d.groupvolhelth[gid] = vids
		d.rlock.Unlock()
		for i = 0; i < minScore; i++ {
			gids = append(gids, gid)
		}
		gidsRepl[gid] = gidRepl
		log.Infof("add gid(%d) minScore(%d)", gid, minScore)
	}
	d.gids = gids
	d.gidRepl = gidsRepl
	log.Infof("Update() gids(%v)", gids)
	return
}

// cal_score algorithm of calculating score
func (d *Dispatcher) calScore(totalAdd, totalAddDelay, restSpace int) (score int) {
	var (
		rsScore, adScore int
	)
	rsScore = (restSpace / int(spaceBenchmark))
	if rsScore > maxScore {
		rsScore = maxScore // more than 32T will be 32T and set score maxScore; less than 32G will be set 0 score;
	}
	if totalAdd != 0 {
		adScore = (((totalAddDelay / nsToMs) / totalAdd) / addDelayBenchmark) * baseAddDelay
		//	log.Infof("space ===%d delay==%d", rsScore, adScore)
		if adScore > maxScore {
			adScore = maxScore // more than 1s will be 1s and set score -maxScore; less than 100ms will be set -0 score;
		}
	}
	score = rsScore - adScore
	return
}

/*
// VolumeId get a volume id.
func (d *Dispatcher) VolumeId(group map[int][]string, storeVolume map[string][]int32) (vid int32, err error) {
	var (
		sid    string
		stores []string
		gid    int
		vids   []int32
	)
	if len(d.gids) == 0 {
		err = errors.ErrStoreNotAvailable
		return
	}
	d.rlock.Lock()
	defer d.rlock.Unlock()
	gid = d.gids[d.rand.Intn(len(d.gids))]
	stores = group[gid]
	if len(stores) == 0 {
		err = errors.ErrZookeeperDataError
		return
	}
	sid = stores[0]
	vids = storeVolume[sid]
	vid = vids[d.rand.Intn(len(vids))]
	return
}
*/

func (d *Dispatcher) VolumeId(exclGid int, dispatcher int, replication int) (vid int32, err error) {
	//dispatcher is score
	if dispatcher == meta.Dispatcher_score {
		if vid, err = d.VolumeIdScoreExcl(exclGid, replication); err != nil {
			log.Errorf("score dispatcher failed err(%v)", err)
		}
	} else {
		//dispatcher is poling
		if vid, err = d.VolumeIdPolingExcl(exclGid, replication); err != nil {
			log.Errorf("score dispatcher failed err(%v)", err)
		}

	}
	return
}

// dispatcher poling get vol id
func (d *Dispatcher) VolumeIdPolingExcl(exclGid int, replication int) (vid int32, err error) {
	var (
		gid, i int
		vids   []int32
		gs     []int

		gidnums int
		gids    []int
		ok      bool
	)

	ok = false
	gids = d.gids
	if len(gids) == 0 {
		err = errors.ErrStoreNotAvailable
		return
	}

	gids = d.replFilter(gids, replication)
	gidnums, gs = d.gidnums(exclGid, gids)
	if gidnums == 0 {
		err = errors.ErrStoreNotAvailable
		return
	}

	sort.Ints(gs)

	for i, gid = range gs {
		if gid == d.polinggid {
			break
		}
	}

	i = i + 1

	if i > (gidnums - 1) {
		i = 0
	}
	d.polinggid = gs[i]
	gid = gs[i]

	d.rlock.Lock()
	if vids, ok = d.groupvolhelth[gid]; !ok {
		err = errors.ErrZookeeperDataError
	}

	defer d.rlock.Unlock()
	if err != nil {
		return
	}

	if len(vids) == 0 {
		err = errors.ErrZookeeperDataError
		log.Errorf("group have no helth volume")
		return
	}

	vid = vids[d.rand.Intn(len(vids))]
	return

}

// group replication filter
func (d *Dispatcher) replFilter(gids []int, replication int) (gs []int) {
	var (
		gid, repl int
		gidRepl   map[int]int
	)
	gidRepl = d.gidRepl

	for _, gid = range gids {
		repl = gidRepl[gid]
		if repl == replication {
			gs = append(gs, gid)
		}
	}

	return
}

// get gid count and filter exclgid
func (d *Dispatcher) gidnums(exclGid int, gids []int) (nums int, gs []int) {
	var (
		num    int
		tmpgid int
	)
	if len(gids) == 0 {
		nums = 0
		return
	}
	num = 0
	tmpgid = -1
	for _, gid := range gids {
		if tmpgid == -1 {
			if gid != exclGid {
				tmpgid = gid
				gs = append(gs, gid)
				num = num + 1
			}
			continue
		}
		if tmpgid != gid {
			if exclGid == gid {
				continue
			}
			tmpgid = gid
			num = num + 1
		}
		gs = append(gs, gid)

	}
	nums = num
	return
}

// VolumeId get a volume id exclude bad vid.  gids is have helth volume ,none helth volume is not in gids.
func (d *Dispatcher) VolumeIdScoreExcl(exclGid int, replication int) (vid int32, err error) {
	var (
		gid  int
		vids []int32

		gidnums  int
		gs, gids []int
		ok       bool
	)

	gids = d.gids
	if len(gids) == 0 {
		err = errors.ErrStoreNotAvailable
		return
	}

	gids = d.replFilter(gids, replication)
	gidnums, gs = d.gidnums(exclGid, gids)

	//d.rlock.Lock()
	//defer d.rlock.Unlock()

	if gidnums == 0 || len(gs) == 0 {
		err = errors.ErrStoreNotAvailable
		log.Errorf("have not available store gidnums:%d,gs:%v,exclGid:%d,gids:%v\n", gidnums, gs, exclGid, gids)
		return
	}
	d.rlock.Lock()
	gid = gs[d.rand.Intn(len(gs))]

	if vids, ok = d.groupvolhelth[gid]; !ok {
		err = errors.ErrZookeeperDataError
	}

	defer d.rlock.Unlock()
	if err != nil {
		return
	}

	if len(vids) == 0 {
		err = errors.ErrZookeeperDataError
		return
	}

	vid = vids[d.rand.Intn(len(vids))]
	return

}
