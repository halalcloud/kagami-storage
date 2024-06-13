package main

import (
	"efs/libs/errors"
	"efs/libs/meta"
	log "efs/log/glog"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

const (
	_pingOk                   = 0
	_rebalanceStopStatus      = "stop"
	_rebalanceDoingStatus     = "doing"
	_rebalanceReadyStatus     = ""
	_dispatcherDefaultModle   = "default"
	_dispatcherRebalanceModle = "rebalance"
)

const (
	_rebalanceMoveReadyStatus  = 0
	_rebalanceMoveingStatus    = 1
	_rebalanceMoveOkStatus     = 2
	_rebalanceMoveFailedStatus = 3
	_rebalanceMovedStatus      = 4
)

var (
	MaxVolumeID        int
	AllVolumeCount     int
	AllFreeVolumeCount int
	StoreRblApi        map[string]string
	StoreAdminApi      map[string]string
	StoreStatApi       map[string]string
	VidStoreRblDisk    map[string]string
	GroupStores        map[string][]string
	StoreRock          map[string]string
	StorePaths         map[string][]string
	StorePathVids      map[string][]string
	VolumeStore        map[string][]string
	OnLineVidStoreids  map[string][]string
	SrcVidStoreid      map[string][]string
	SrcVidDestVid      map[string]string
	SrcVidInfos        map[string]meta.VidValue //TODO
	GroupSizeR1        map[string]meta.GroupSizeInfo
	GroupSizeR2        map[string]meta.GroupSizeInfo
	GroupSizeR3        map[string]meta.GroupSizeInfo
)

type server struct {
	rb *Rebalance
}

// StartApi start api http listen.
func StartApi(addr string, MaxStoreRebalanceThreadNum int, MaxDiskRebalanceThreadNu int, rb *Rebalance) {
	var (
		s = &server{rb: rb}
		//rblState string
		err error
		ok  bool
	)

	if _, ok = s.rebalanceInit(); ok == false {
		log.Errorf("StartApi rebalanceInit failed!\n")
		return
	}
	if err = s.getEfsMeta(); err != nil {
		log.Errorf("StartApi: getEfsMeta failed (%s)", err.Error())
		return
	}
	/*
		if rblState == "" {
			if err = s.AllotRebalance(); err != nil {
				log.Errorf("StartApi: AllotRebalance failed")
				return
			}
		}*/

	go s.checkRebalanceStatus(MaxStoreRebalanceThreadNum, MaxDiskRebalanceThreadNu)
	go func() {
		var (
			err      error
			serveMux = http.NewServeMux()
		)
		serveMux.HandleFunc("/startRebalance", s.startRebalance)
		serveMux.HandleFunc("/stopRebalance", s.stopRebalance)
		serveMux.HandleFunc("/finishRebalance", s.finishRebalance)
		serveMux.HandleFunc("/ping", s.ping)
		if err = http.ListenAndServe(addr, serveMux); err != nil {
			log.Errorf("http.ListenAndServe(\"%s\") error(%v)", addr, err)
			return
		}
	}()
	return
}

func respError(wr http.ResponseWriter, data []byte, code int) {
	wr.Header().Set("Content-Type", "application/json; charset=utf-8")
	wr.WriteHeader(code)
	wr.Write(data)
}

func (s *server) getEfsMeta() (err error) {
	StoreRblApi = make(map[string]string)
	StoreAdminApi = make(map[string]string)
	StoreStatApi = make(map[string]string)
	VidStoreRblDisk = make(map[string]string)
	SrcVidStoreid = make(map[string][]string)
	SrcVidDestVid = make(map[string]string)
	SrcVidInfos = make(map[string]meta.VidValue)
	GroupSizeR1 = make(map[string]meta.GroupSizeInfo)
	GroupSizeR2 = make(map[string]meta.GroupSizeInfo)
	GroupSizeR3 = make(map[string]meta.GroupSizeInfo)
	GroupStores = make(map[string][]string)
	VolumeStore = make(map[string][]string)
	StoreRock = make(map[string]string)
	StorePaths = make(map[string][]string)
	StorePathVids = make(map[string][]string)
	OnLineVidStoreids = make(map[string][]string)

	if err = s.rb.DoGetEfsMeta(); err != nil {
		log.Errorf("getEfsMeta: DoGetEfsMeta failed")
		return
	}
	return
}

func (s *server) rebalanceInit() (state string, ok bool) {
	var (
		err error
	)

	if state, err = s.rb.GetRebalanceStatus(); err != nil {
		ok = false
		log.Errorf("rebalanceInit: GetRebalanceStatus failed (%s)!", err.Error())
		return
	}
	//	state = ""
	if state == "" {
		if err = s.rb.CleanRebalanceChildren(); err != nil {
			ok = false
			log.Errorf("rebalanceInit: Failed (%s)", err.Error())
			return
		}

	}

	return state, true
	//return state, false

}

func (s *server) AllotRebalance() (err error) {

	if err = s.rb.AllotNewVolumeToOnLine(); err != nil {
		log.Errorf("AllotRebalance: AllotNewVolumeToOnLine failed")
		return
	}
	if err = s.rb.DoAddNewVolumeToOnLine(); err != nil {
		log.Errorf("AllotRebalance: DoAddNewVolumeToOnLine failed")
		return
	}
	if err = s.rb.DoSetRebalanceMeta(); err != nil {
		log.Errorf("AllotRebalance: DoSetRebalanceMeta failed")
		return
	}
	return
}

func (s *server) doRebalance(MaxStoreRebalanceThreadNum int, MaxDiskRebalanceThreadNu int) {
	var (
		storeRblCount map[string]int
		distRblCount  map[string]int
		MoveReadyVid  map[string]meta.VidInfo
		MoveingVid    map[string]meta.VidInfo
		MoveOkVid     map[string]meta.VidInfo
		MoveFailedVid map[string]meta.VidInfo
		MovedVid      map[string]meta.VidInfo
		err           error
		state         string
	)

	storeRblCount = make(map[string]int)
	distRblCount = make(map[string]int)
	MoveReadyVid = make(map[string]meta.VidInfo)
	MoveingVid = make(map[string]meta.VidInfo)
	MoveOkVid = make(map[string]meta.VidInfo)
	MoveFailedVid = make(map[string]meta.VidInfo)
	MovedVid = make(map[string]meta.VidInfo)

	if err = s.rb.GetRebalanceValueInfo(storeRblCount, distRblCount, MoveReadyVid, MoveingVid, MoveOkVid, MoveFailedVid, MovedVid); err != nil {
		log.Errorf("doRebalance: getRebalanceValueInfo failed (%s)", err.Error())
		return
	}

	if len(MoveReadyVid) == 0 && len(MoveingVid) == 0 && len(MoveOkVid) == 0 && len(MoveFailedVid) == 0 {
		if err = s.rb.SetRebalanceStatus(_rebalanceReadyStatus); err != nil {
			log.Errorf("finishRebalance Failed (%s)", err.Error())
		}
		if err = s.rb.SetDispatcherModle(fmt.Sprintf("%d", meta.Dispatcher_score)); err != nil {
			log.Errorf("stopRebalance Failed (%s)", err.Error())
		}
		return
	}

	for vd, vi := range MoveOkVid {
		for sd, si := range vi.Storesinfo {
			if si.MoveStatus == _rebalanceMoveOkStatus {
				//add dest vid add in /volume/srcstoreid/destvid
				if err = s.rb.DoAddRebalancedVolume(vi.Vidinfo.DestVid, sd); err != nil {
					log.Errorf("doRebalance: s.rb.DoAddRebalancedVolume failed vid=%s, storeid =%s, err =%v", vd, sd, err)
					return
				}
				if err = s.rb.DoSetVidStoreRebalanceStatus(vd, sd, si, _rebalanceMovedStatus); err != nil {
					log.Errorf("doRebalance: s.rb.DoSetVidStoreRebalancedStatus failed  _rebalanceMovedStatus vid = %s, storeid = %s, err =%v", vd, sd, err)
					return
				}
			}
		}
	}

	if state, err = s.rb.GetRebalanceStatus(); err != nil {

		log.Errorf("dorebalance: GetRebalanceStatus failed (%s)!", err.Error())
		return
	}
	if state != _rebalanceDoingStatus {
		return
	}

	fmt.Println("send move data ================")
	for vd, vi := range MoveReadyVid {
		var (
			s_i           int
			filter        []string
			tmpstoreids   []string
			fdeststoreids []string
			ok            bool
		)

		//filter already moving deststoreid
		tmpstoreids = vi.Vidinfo.DestStoreId
		for _, si1 := range vi.Storesinfo {
			if si1.MoveStatus != _rebalanceMoveReadyStatus {
				filter = append(filter, si1.Deststoreid)
			}
		}
		for _, indexstoreid := range tmpstoreids {
			for _, fstoreid := range filter {
				if indexstoreid == fstoreid {
					ok = true
					break
				}
			}
			if !ok {
				fdeststoreids = append(fdeststoreids, indexstoreid)
			} else {
				ok = false
			}
		}

		for sd, si := range vi.Storesinfo {
			//dest storeid is send so not send

			if si.MoveStatus == _rebalanceMoveReadyStatus && storeRblCount[sd] < MaxStoreRebalanceThreadNum {
				if path := VidStoreRblDisk[vd+"_"+sd]; path != "" {
					if distRblCount[vd+"_"+sd+"_"+path] < MaxDiskRebalanceThreadNu {
						fmt.Println("send move data =************send=============", vi)
						if err = s.rb.SendRebalanceToStore(vd, fdeststoreids[s_i], vi.Vidinfo, StoreRblApi[sd]); err != nil {
							log.Errorf("doRebalance: s.rb.SendRebalanceToStore failed vid =%s, storeid = %s, err (%v)", vd, sd, err)
							return
						}
						s_i += 1
						storeRblCount[sd] += 1
						distRblCount[vd+"_"+sd+"_"+path] += 1
						if err = s.rb.DoSetVidStoreRebalanceStatus(vd, sd, si, _rebalanceMoveingStatus); err != nil {
							log.Errorf("doRebalance: s.rb.DoSetVidStoreRebalancedStatus failed  _rebalanceMoveingStatus vid = %s, storeid = %s, err =%v", vd, sd, err)
							return
						}
					}
				}
			}
		}
	}
}

func (s *server) checkRebalanceStatus(MaxStoreRebalanceThreadNum int, MaxDiskRebalanceThreadNu int) {
	var (
		err      error
		rblState string
	)

	for {
		if rblState, err = s.rb.GetRebalanceStatus(); err != nil {
			log.Errorf("startRebalance: GetRebalanceStatus failed (%s)!", err.Error())
			time.Sleep(1 * time.Minute)
			continue
		}
		if rblState == "" {
			continue
		}

		s.doRebalance(MaxStoreRebalanceThreadNum, MaxDiskRebalanceThreadNu)
		time.Sleep(3 * time.Minute)
	}
}

func (s *server) startRebalance(wr http.ResponseWriter, r *http.Request) {
	var (
		err      error
		ret      meta.StopRblFailed
		retJson  []byte
		status   = http.StatusOK
		rblState string
	)
	log.Info("Start rebalance ...")
	if rblState, err = s.rb.GetRebalanceStatus(); err != nil {
		err = errors.ErrGetRblStatus
		status = errors.RetGetRblStatusFailed
		log.Errorf("startRebalance: GetRebalanceStatus failed (%s)!", err.Error())
		goto failed
	}
	if rblState == "" {
		if err = s.rb.CleanRebalanceChildren(); err != nil {
			err = errors.ErrCleanRblChild
			status = errors.RetCleanRblChildrenFailed
			log.Errorf("finishRebalance Failed (%s)", err.Error())
			goto failed
		}
		if err = s.getEfsMeta(); err != nil {
			log.Errorf("StartApi: getEfsMeta failed (%s)", err.Error())
			err = errors.ErrStartRblFailed
			status = errors.RetStartRblFailed
			goto failed
		}
		if err = s.AllotRebalance(); err != nil {
			log.Errorf("StartApi: AllotRebalance failed")
			err = errors.ErrStartRblFailed
			status = errors.RetStartRblFailed
			goto failed
		}
	} else if rblState == _rebalanceStopStatus {
		log.Info("rebalance status from stop to start ...")
	} else {
		err = errors.ErrRblAlreadyStart
		status = errors.RetRblAlreadyStart
		goto failed
	}
	if err = s.rb.SetRebalanceStatus(_rebalanceDoingStatus); err != nil {
		err = errors.ErrSetRblStatus
		status = errors.RetSetRblStatusFailed
		log.Errorf("stopRebalance Failed (%s)", err.Error())
		goto failed
	}
	if err = s.rb.SetDispatcherModle(fmt.Sprintf("%d", meta.Dispatcher_polling)); err != nil {
		err = errors.ErrSetRblStatus
		status = errors.RetSetRblStatusFailed
		log.Errorf("stopRebalance Failed (%s)", err.Error())
		goto failed
	}
	return
failed:
	ret.Code = status
	ret.Error = err.Error()
	if retJson, err = json.Marshal(ret); err != nil {
		log.Errorf("stopRebalance json.Marshal() error(%v)", err)
		return
	}
	respError(wr, retJson, status)
}

func (s *server) stopRebalance(wr http.ResponseWriter, r *http.Request) {
	var (
		err     error
		ret     meta.StopRblFailed
		retJson []byte
		status  = http.StatusOK
	)
	if err = s.rb.SetRebalanceStatus(_rebalanceStopStatus); err != nil {
		err = errors.ErrSetRblStatus
		status = errors.RetSetRblStatusFailed
		log.Errorf("stopRebalance Failed (%s)", err.Error())
		goto failed
	}
	if err = s.rb.SetDispatcherModle(fmt.Sprintf("%d", meta.Dispatcher_score)); err != nil {
		err = errors.ErrSetRblStatus
		status = errors.RetSetRblStatusFailed
		log.Errorf("stopRebalance Failed (%s)", err.Error())
		goto failed
	}
	return
failed:
	ret.Code = status
	ret.Error = err.Error()
	if retJson, err = json.Marshal(ret); err != nil {
		log.Errorf("stopRebalance json.Marshal() error(%v)", err)
		return
	}
	respError(wr, retJson, status)
	return
}

func (s *server) finishRebalance(wr http.ResponseWriter, r *http.Request) {
	var (
		err     error
		ret     meta.FinishRblFailed
		retJson []byte
		status  = http.StatusOK
	)

	if err = s.rb.CleanRebalanceChildren(); err != nil {
		err = errors.ErrCleanRblChild
		status = errors.RetCleanRblChildrenFailed
		log.Errorf("finishRebalance Failed (%s)", err.Error())
		goto failed
	}

	if err = s.rb.SetRebalanceStatus(_rebalanceReadyStatus); err != nil {
		err = errors.ErrSetRblStatus
		status = errors.RetSetRblStatusFailed
		log.Errorf("finishRebalance Failed (%s)", err.Error())
		goto failed
	}
	if err = s.rb.SetDispatcherModle(fmt.Sprintf("%d", meta.Dispatcher_score)); err != nil {
		err = errors.ErrSetRblStatus
		status = errors.RetSetRblStatusFailed
		log.Errorf("stopRebalance Failed (%s)", err.Error())
		goto failed
	}

	return

failed:
	ret.Code = status
	ret.Error = err.Error()
	if retJson, err = json.Marshal(ret); err != nil {
		log.Errorf("finishRebalance json.Marshal() error(%v)", err)
		return
	}
	respError(wr, retJson, status)
	return
}

func (s *server) ping(wr http.ResponseWriter, r *http.Request) {
	var (
		byteJson []byte
		res      = map[string]interface{}{"code": _pingOk}
		err      error
	)
	if byteJson, err = json.Marshal(res); err != nil {
		log.Errorf("json.Marshal(\"%v\") failed (%v)", res, err)
		return
	}
	wr.Header().Set("Content-Type", "application/json;charset=utf-8")
	if _, err = wr.Write(byteJson); err != nil {
		log.Errorf("HttpWriter Write error(%v)", err)
	}
	return
}
