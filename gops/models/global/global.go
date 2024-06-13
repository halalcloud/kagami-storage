package global

import (
	"efs/gops/models/types"
	"sync"
)

var (
	MAX_GROUP_ID  uint64 = 0
	MAX_VOLUME_ID uint64 = 0
	STORES        map[string]*types.Store
	//IN_GROUP_STORES  map[string]*types.Store
	IN_GROUP_STORES []string
	GROUPS          map[uint64]*types.Group
	VOLUMES         map[uint64]*types.Volume
	Rack_lock       *sync.RWMutex
	Store_lock      *sync.RWMutex
	Group_lock      *sync.RWMutex
	Volume_lock     *sync.RWMutex

	Statusfail    int = 0 // faild
	Statusfull    int = 1 // volume is full only read for volume
	Statushealth  int = 2 // helth
	Statuscanread int = 3 // only can ready store is not down mybe disk is error
	Statusrecover int = 4 //volume recover

	VOLUME_SPACE       uint64 = 32 * 1024 * 1024 * 1024 //32GB
	VOLUME_INDEX_SPACE uint64 = 100 * 1024 * 1024       //100M
	ROLE_ADMIN         int32  = 1
	ROLE_USER          int32  = 2
	RECEIVE_ALARM      int32  = 1
	NRECEIVE_ALARM     int32  = 2
)

func init() {
	Store_lock = new(sync.RWMutex)
	Group_lock = new(sync.RWMutex)
	Volume_lock = new(sync.RWMutex)
}

func Statustostring(status int) (alarmstatus string) {
	if status == Statusfail {
		alarmstatus = "fail stat"
	} else if status == Statusfull {
		alarmstatus = "space full"
	} else if status == Statushealth {
		alarmstatus = "health"
	} else {
		alarmstatus = "only read"
	}

	return
}

//exist in group
func IsInGroup(storeid string) (exist bool) {
	inGroupStores := IN_GROUP_STORES

	if len(inGroupStores) <= 0 {
		return false
	}

	for _, v := range inGroupStores {
		if storeid == v {
			return true
		}
	}

	return false
}
