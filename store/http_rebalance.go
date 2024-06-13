package main

import (
	"bytes"
	"efs/libs/errors"
	"efs/libs/meta"
	myos "efs/store/os"
	"efs/store/volume"
	"encoding/json"
	syserrors "errors"
	"fmt"
	"io"
	"io/ioutil"
	"mime/multipart"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	itime "github.com/Terry-Mao/marmot/time"

	log "efs/log/glog"
)

const (
	//api
	_dest_movedata = "http://%s/movevolume_recvdata"
	_movedata      = "http://%s/movedata"
	_recovery_dest = "http://%s/recovery_dest"

	_finish                  = 0
	_Recover_timeout         = 600 //recovery timeout 600s
	_TimeoutCleanVolMoveFail = 120 //clean volume revcover move timeout 120s
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

//rebalance move source local
type volumerebalance struct {
	vid         int32
	newvid      int32
	deststoreid string
	status      bool
	//	control     chan int
}

//rebalance move dest local
type volumetmp struct {
	vid                 int32
	file_tmpvolume      string
	file_tmpvolumeindex string
	ftvol               *os.File
	ftvolindex          *os.File
	updatetime          int64
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

	//log.Infof("%s", ru)
	if method == "GET" {
		if req, err = http.NewRequest("GET", ru, nil); err != nil {
			return
		}
	} else {
		if buf == nil {
			if req, err = http.NewRequest("POST", ru, strings.NewReader(enc)); err != nil {
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
	td := _timer.Start(60*time.Second, func() {
		_canceler(req)
	})
	if resp, err = _client.Do(req); err != nil {
		log.Errorf("_client.Do(%s) error(%v)", ru, err)
		return
	}
	td.Stop()
	defer resp.Body.Close()
	if res == nil {
		return
	}
	if resp.StatusCode != http.StatusOK {
		log.Errorf("_client.Do(%s) status: %d", ru, resp.StatusCode)
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

func (s *Server) startRebalance() {
	var (
		err      error
		serveMux = http.NewServeMux()
		server   = &http.Server{
			Addr:    s.conf.ApiListen,
			Handler: serveMux,
			// TODO read/write timeout
		}
	)
	serveMux.HandleFunc("/movevolume", s.movevolume)
	//serveMux.HandleFunc("/stop_movevolume", s.stop_movevolume)
	serveMux.HandleFunc("/movevolume_recvdata", s.movevolume_recvdata)
	serveMux.HandleFunc("/recovery_req", s.recovery_req)
	serveMux.HandleFunc("/recovery_dest", s.recovery_dest)

	if err = server.Serve(s.rebalanceSvr); err != nil {
		log.Errorf("server.Serve() error(%v)", err)
	}
	log.Info("http rebalance stop")
	return
}

func (s *Server) readalldata(rd io.Reader, size int64) (data []byte, err error) {
	var (
		recvdata []byte
	)

	recvdata = make([]byte, size)

	if _, err = rd.Read(recvdata); err != nil {
		log.Errorf("recv data read form table faild")
	}

	data = recvdata
	return

}

//recv data from  move volume
func (s *Server) movevolume_recvdata(wr http.ResponseWriter, r *http.Request) {
	var (
		vid      int64
		offset   int64
		end      int64
		size     int64
		file     multipart.File
		res      = map[string]interface{}{}
		filetype string
		str      string
		data     []byte
		err      error
	)

	if r.Method != "POST" {
		http.Error(wr, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	defer HttpPostWriter(r, wr, time.Now(), &err, res)

	/*
		if !s.wl.Allow() {
			err = errors.ErrServiceUnavailable
			return
		}
	*/
	str = r.FormValue("mvid")
	if vid, err = strconv.ParseInt(str, 10, 32); err != nil {
		log.Errorf("strconv.ParseInt(\"%s\") error(%v)", str, err)
		err = errors.ErrParam
		return
	}
	filetype = r.FormValue("filetype")
	str = r.FormValue("offset")
	if offset, err = strconv.ParseInt(str, 10, 64); err != nil {
		log.Errorf("strconv.ParseInt(\"%s\") error(%v)", str, err)
		err = errors.ErrParam
		return
	}
	str = r.FormValue("end")
	if end, err = strconv.ParseInt(str, 10, 32); err != nil {
		log.Errorf("strconv.ParseInt(\"%s\") error(%v)", str, err)
		err = errors.ErrParam
		return
	}

	if file, _, err = r.FormFile("file"); err != nil {
		log.Errorf("r.FormFile() error(%v)", err)
		err = errors.ErrInternal
		return
	}

	if size, err = checkFileSize(file, s.conf.NeedleMaxSize); err == nil {
		if size != 0 {
			data, err = s.readalldata(file, size)
			if err != nil {
				err = errors.ErrParam
				return
			}
		}
	} else {
		err = errors.ErrParam
		return
	}

	if err = s.store.movevolume_writedata(int32(vid), filetype, offset, int32(end), data, size); err != nil {
		log.Errorf("do move data failed")
	}

	file.Close()
	return

}

//add volume in local recovery index and open file
func (s *Store) add_recovery_volume(vid int32) (voltmp *volumetmp, err error) {
	var (
		vol   *volumetmp
		fv, v *volume.Volume
	)
	s.flock.Lock()
	if fv = s.getlessdiskvolume(); fv == nil {
		err = syserrors.New("have no free volume")
		log.Errorf("get freevolume failed error(%v)", err)
		s.flock.Unlock()
		return
	}
	if err = s.saveFreeVolumeIndex(); err != nil {
		log.Errorf("get freevolume failed error(%v)", err)
		s.flock.Unlock()
		return
	}
	s.flock.Unlock()

	vol = new(volumetmp)
	vol.file_tmpvolume = fv.Block.File
	vol.file_tmpvolumeindex = fv.Indexer.File

	//this freevolume is used for move recover tmp volume,so close this freevolume
	fv.Close()

	vol.vid = vid
	if vol.ftvol, err = os.OpenFile(fv.Block.File, os.O_WRONLY|os.O_CREATE|myos.O_NOATIME, 0664); err != nil {
		log.Errorf("rebalance dest open file %s failed error(%v)", fv.Block.File, err)
		return
	}
	if vol.ftvolindex, err = os.OpenFile(fv.Indexer.File, os.O_WRONLY|os.O_CREATE|myos.O_NOATIME, 0664); err != nil {
		log.Errorf("rebalance dest open file %s failed error(%v)", fv.Indexer.File, err)
		return
	}

	s.rclock.Lock()
	s.recoveryvolumes = append(s.recoveryvolumes, vol)
	s.saveRecoveryVolumeIndex()
	s.rclock.Unlock()
	voltmp = vol
	//modify volume recover dest volume status
	if v = s.Volumes[vid]; v != nil {
		v.Modify_doing_recovery_status()
	}

	return
}

func (s *Store) CleanFailVolmeMove() {
	var (
		tmps []*volumetmp
		err  error
	)
	for {
		s.rclock.Lock()
		tmps = s.recoveryvolumes
		s.rclock.Unlock()
		for _, vol := range tmps {
			//log.Errorf("check id %d,time %d", vol.vid, (time.Now().Unix() - vol.updatetime))
			if (time.Now().Unix() - vol.updatetime) > _TimeoutCleanVolMoveFail {
				bdir, idir := filepath.Dir(vol.file_tmpvolume), filepath.Dir(vol.file_tmpvolumeindex)
				s.rclock.Lock()
				if err = s.destoryRecoveryVolumeIndex(vol.vid); err != nil {
					s.rclock.Unlock()
					continue
				}
				s.rclock.Unlock()
				os.Remove(vol.file_tmpvolume)
				os.Remove(vol.file_tmpvolumeindex)
				log.Errorf("timout recover add free volume %s id %d", bdir, vol.vid)
				_, err = s.AddFreeVolume(1, bdir, idir)
				if err != nil {
					log.Recoverf("recover dest volume add freevolue bdir %s idir %s faild", bdir, idir)
				}
			}
		}

		time.Sleep(60 * time.Second)
	}
}

func (s *Store) set_movedest_volume_updatetime(vid int32) {
	s.rclock.Lock()
	for _, vol := range s.recoveryvolumes {
		if vol.vid == vid {
			vol.updatetime = time.Now().Unix()
			break
		}
	}
	s.rclock.Unlock()
}

//add volume in local movedest index and open file
func (s *Store) add_movedest_volume(vid int32) (voltmp *volumetmp, err error) {
	var (
		vol *volumetmp
		fv  *volume.Volume
	)
	s.flock.Lock()
	if fv = s.getlessdiskvolume(); fv == nil {
		log.Errorf("get freevolume failed error(%v)", err)
		s.flock.Unlock()
		return
	}
	if err = s.saveFreeVolumeIndex(); err != nil {
		log.Errorf("get freevolume failed error(%v)", err)
		s.flock.Unlock()
		return
	}
	s.flock.Unlock()
	vol = new(volumetmp)
	vol.file_tmpvolume = fv.Block.File
	vol.file_tmpvolumeindex = fv.Indexer.File
	//this freevolume is used for move rebalance tmp volume,so CLOSE this freevolume
	fv.Close()
	vol.vid = vid
	if vol.ftvol, err = os.OpenFile(fv.Block.File, os.O_WRONLY|os.O_CREATE|myos.O_NOATIME, 0664); err != nil {
		log.Errorf("rebalance dest open file %s failed error(%v)", fv.Block.File, err)
		return
	}
	if vol.ftvolindex, err = os.OpenFile(fv.Indexer.File, os.O_WRONLY|os.O_CREATE|myos.O_NOATIME, 0664); err != nil {
		log.Errorf("rebalance dest open file %s failed error(%v)", fv.Indexer.File, err)
		return
	}

	s.dmlock.Lock()
	s.rebalancedestvolumes = append(s.rebalancedestvolumes, vol)
	s.saveMoveDestVolumeIndex()
	s.dmlock.Unlock()
	voltmp = vol
	return
}

//destory recovery volume index local not remove file
func (s *Store) destoryRecoveryVolumeIndex(vid int32) (err error) {
	var (
		i    int
		vol  *volumetmp
		find bool
	)

	for i, vol = range s.recoveryvolumes {
		if vol.vid == vid {
			find = true
			break
		}
	}
	if !find {
		return
	}

	s.recoveryvolumes = append(s.recoveryvolumes[:i], s.recoveryvolumes[(i+1):]...)
	if err = s.saveRecoveryVolumeIndex(); err != nil {
		log.Errorf("save move destvolume index local failed")
	}
	vol.ftvol.Close()
	vol.ftvolindex.Close()

	return
}

//destory dest volume index local
func (s *Store) destoryDestVolumeIndex(vid int32) (err error) {
	var (
		i    int
		vol  *volumetmp
		find bool
	)

	s.dmlock.Lock()
	defer s.dmlock.Unlock()
	for i, vol = range s.rebalancedestvolumes {
		if vol.vid == vid {
			find = true
			break
		}
	}
	if !find {
		return
	}

	s.rebalancedestvolumes = append(s.rebalancedestvolumes[:i], s.rebalancedestvolumes[(i+1):]...)
	if err = s.saveMoveDestVolumeIndex(); err != nil {
		log.Errorf("save move destvolume index local failed")
	}
	vol.ftvol.Close()
	vol.ftvolindex.Close()

	return
}

func (s *Store) movevolume_writedata(vid int32, filetype string, offset int64, end int32, data []byte, size int64) (err error) {
	var (
		vol                                      *volumetmp
		find                                     bool
		bdir, idir, bfile, ifile, nbfile, nifile string
		i                                        int
		v                                        *volume.Volume
	)
	s.dmlock.Lock()
	for _, vol = range s.rebalancedestvolumes {
		if vol.vid == vid {
			find = true
			break
		}
	}
	s.dmlock.Unlock()

	if !find {
		// not have move dest volume file add this task
		if vol, err = s.add_movedest_volume(vid); err != nil {
			return
		}
	}
	if len(data) != 0 {
		if filetype == "volume" {
			if _, err = vol.ftvol.WriteAt(data, offset); err != nil {
				log.Errorf("wirte file %s failed error(%v)", vol.file_tmpvolume, err)
				return
			}
		} else {
			if _, err = vol.ftvolindex.WriteAt(data, offset); err != nil {
				log.Errorf("wirte file %s failed error(%v)", vol.file_tmpvolumeindex, err)
				return
			}
		}
	}
	// write volume file and volume index sucess
	if end == 1 && filetype == "volumeindex" {
		bfile, ifile = vol.file_tmpvolume, vol.file_tmpvolumeindex
		bdir, idir = filepath.Dir(bfile), filepath.Dir(ifile)
		for {
			nbfile, nifile = s.file(vid, bdir, idir, i)
			if !myos.Exist(nbfile) && !myos.Exist(nifile) {
				break
			}
			i++
		}
		// rename tmp volume name to volume name
		if err = os.Rename(ifile, nifile); err != nil {
			log.Recoverf("move volume dest vid %d os.Rename(\"%s\", \"%s\") error(%v)", vid, ifile, nifile, err)
			return
		}
		if err = os.Rename(bfile, nbfile); err != nil {
			log.Recoverf("move volume dest vid %d os.Rename(\"%s\", \"%s\") error(%v)", vid, bfile, nbfile, err)
			return
		}
		// add a new volume
		if v, err = newVolume(vid, nbfile, nifile, s.conf); err != nil {
			log.Recoverf("move volume dest vid %d add new volume failed", vid)
			return
		}
		s.vlock.Lock()
		s.addVolume(vid, v)
		if err = s.saveVolumeIndex(); err == nil {
			if err = s.zk.AddVolume(vid, v.Meta()); err != nil {
				log.Recoverf("rebalance add volume in zookeeper /rack/storeid/vid failed vid %d", vid)
			}
			// add store in zookeeper to /volume/vid/storeid
			if err = s.zk.AddVolStore(vid); err != nil {
				log.Recoverf("rebalance add volume in zookeeper /voluem/vid/store failed vid %d", vid)
			}

		} else {
			s.vlock.Unlock()
			log.Recoverf("rebalance add volume: %d error(%v), local index or zookeeper index may save failed", vid, err)
			return
		}
		s.vlock.Unlock()
		//destory destvolume index file
		if err = s.destoryDestVolumeIndex(vid); err != nil {
			log.Recoverf("detory destvolume index vid %d failed", vid)
			err = nil
			return
		}
		log.Infof("move volume dest vid %d sucess", vid)

	}
	return

}

//request rebalance move volume
func (s *Server) movevolume(wr http.ResponseWriter, r *http.Request) {
	var (
		err         error
		vid         int64
		newvid      int64
		res         = map[string]interface{}{}
		deststoreid string
		rack        string
		str         string
	)

	defer HttpPostWriter(r, wr, time.Now(), &err, res)
	if vid, err = strconv.ParseInt(r.FormValue("mvolume"), 10, 32); err != nil {
		log.Errorf("strconv.ParseInt(\"%s\") error(%v)", r.FormValue("vid"), err)
		err = errors.ErrParam
		return
	}

	if newvid, err = strconv.ParseInt(r.FormValue("newvolume"), 10, 32); err != nil {
		log.Errorf("strconv.ParseInt(\"%s\") error(%v)", r.FormValue("vid"), err)
		err = errors.ErrParam
		return
	}

	deststoreid = r.FormValue("deststore")
	rack = r.FormValue("rack")
	str = path.Join(rack, deststoreid)

	err = s.store.addmvvolume(int32(vid), int32(newvid), str)

}

func (s *Server) recovery_req(wr http.ResponseWriter, r *http.Request) {
	var (
		err         error
		vid         int64
		deststoreid string
		rack, str   string

		res = map[string]interface{}{}
	)
	defer HttpPostWriter(r, wr, time.Now(), &err, res)
	if vid, err = strconv.ParseInt(r.FormValue("vid"), 10, 32); err != nil {
		log.Errorf("strconv.ParseInt(\"%s\") error(%v)", r.FormValue("vid"), err)
		err = errors.ErrParam
		return
	}
	deststoreid = r.FormValue("deststore")
	rack = r.FormValue("rack")
	str = path.Join(rack, deststoreid)
	if err = s.store.recover_init(int32(vid), deststoreid); err != nil {
		log.Errorf("recovery init failed ")
		err = errors.ErrParam
		return
	}

	go s.store.recovery_volume(int32(vid), str)
}

func (s *Store) Check_recovertask(vid int32, deststoreid string) (exsit bool) {
	var (
		err   error
		mstat *meta.Recoverystat
	)
	mstat, err = s.zk.GetRecoveryMovestat(vid, deststoreid)
	if err != nil {
		//log.Errorf("get vid %d destoreid %s failed %v", vid, deststoreid, err)
		exsit = false
		return
	}
	if mstat != nil {
		if mstat.Utime+_Recover_timeout < time.Now().Unix() {
			exsit = false
		} else {
			exsit = true
		}
	}
	return
}

func (s *Store) recover_init(vid int32, deststoreid string) (err error) {
	var (
		repath string
		stat   meta.Recoverystat

		//mvolume *volumerebalance
	//	flag    = false
	)
	/*
		s.mlock.Lock()
		for _, mvolume = range s.rebalancevolumes {
			if mvolume.vid == vid {
				flag = true
				break
			}
		}
		s.mlock.Unlock()
		if flag {
			err = errors.ERRVolumeExist
			return
		}
	*/
	if s.Check_recovertask(vid, deststoreid) {
		err = errors.ERRVolumeNotExist
		return
	}
	stat.Srcstoreid = s.conf.Zookeeper.ServerId
	repath = s.conf.Zookeeper.Recovery + "/" + fmt.Sprintf("%d", vid) + "/" + deststoreid
	if err = s.zk.CreatePath(repath); err != nil {
		log.Errorf("init creat zk path %s failed err (%v)", repath, err)
		return
	}
	stat.Srcstoreid = s.conf.Zookeeper.ServerId
	stat.Utime = time.Now().Unix()
	if err = s.zk.SetRecoveryMovestat(vid, deststoreid, &stat); err != nil {
		log.Errorf("set recovery stat failed ")
	}
	return
}

func (s *Store) recovery_volume(vid int32, str string) {
	var (
		rebalanceapi, uri string
		params            = url.Values{}
		res               meta.MoveDataRes
		v                 *volume.Volume
		err1              error
		err               error
		stat              meta.Recoverystat
		deststoreid       string
	)

	rebalanceapi, err = s.zk.Getrebalanceapi(str)
	if err != nil {
		return
	}
	uri = fmt.Sprintf(_recovery_dest, rebalanceapi)
	params.Set("mvid", fmt.Sprintf("%d", vid))

	if v = s.Volumes[vid]; v == nil {
		//??????????
		err = errors.ERRVolumeNotExist
		log.Errorf("have no this volid %d", vid)
		stat.ReStatus = meta.MoveFail
		stat.Utime = time.Now().Unix()
		s.zk.SetRecoveryMovestat(vid, deststoreid, &stat)
		return
	} else {
		if v.Compact {
			log.Infof("volid %d is compact", vid)
			stat.ReStatus = meta.MoveFail
			stat.Utime = time.Now().Unix()
			s.zk.SetRecoveryMovestat(vid, deststoreid, &stat)
			return
		}
	}

	stat.Srcstoreid = s.conf.Zookeeper.ServerId
	t := strings.SplitN(str[1:], "/", 2)
	deststoreid = t[1]
	v.Moveref()
	// read data send, send volume file
	err1 = v.Movevolume(v.Block.File, func(data []byte, offset int64, total int64, end bool) (err error) {
		//params.Set("offset", fmt.Sprintf("%ld", offset))
		off := strconv.FormatInt(offset, 10)
		params.Set("offset", off)
		params.Set("filetype", "volume")
		if end {
			params.Set("end", "1")
		} else {
			params.Set("end", "0")
		}

		if err = Http("POST", uri, params, data, &res); err != nil {
			log.Errorf("GET called Http uri %s error(%v)", uri, err)
			return
		}
		if res.Ret != errors.RetOK || err != nil {
			if err == nil {
				err = errors.ErrInternal
				log.Errorf("send volume %s faild (%v)", v.Block.File, err)
			}
			stat.ReStatus = meta.MoveFail
			stat.Utime = time.Now().Unix()
			s.zk.SetRecoveryMovestat(vid, deststoreid, &stat)
			time.Sleep(100 * time.Millisecond)

			return
		}
		stat.ReStatus = meta.Moving
		stat.MoveTotalData = total
		stat.MoveData = offset
		stat.Utime = time.Now().Unix()

		s.zk.SetRecoveryMovestat(vid, deststoreid, &stat)
		return
	})

	if err1 != nil {
		stat.ReStatus = meta.MoveFail
		stat.Utime = time.Now().Unix()
		s.zk.SetRecoveryMovestat(vid, deststoreid, &stat)
		v.Moveunref()
		return
	}

	// send volume index data
	err1 = v.Movevolume(v.Indexer.File, func(data []byte, offset int64, total int64, end bool) (err error) {
		//params.Set("offset", fmt.Sprintf("%ld", offset))
		off := strconv.FormatInt(offset, 10)
		params.Set("offset", off)

		params.Set("filetype", "volumeindex")
		if end {
			params.Set("end", "1")
		} else {
			params.Set("end", "0")
		}

		if err = Http("POST", uri, params, data, &res); err != nil {
			log.Errorf("GET called Http uri %s error(%v)", uri, err)
			return
		}
		if res.Ret != errors.RetOK || err != nil {
			if err != nil {
				err = errors.ErrInternal
				log.Errorf("send volume %s faild (%v)", v.Indexer.File, err)
			}

			stat.ReStatus = meta.MoveFail
			stat.Utime = time.Now().Unix()
			s.zk.SetRecoveryMovestat(vid, deststoreid, &stat)
			return
		}

		stat.ReStatus = meta.Moving
		//stat.MoveTotalData = total
		//stat.MoveData = offset
		stat.Utime = time.Now().Unix()

		s.zk.SetRecoveryMovestat(vid, deststoreid, &stat)
		return

	})

	if err1 != nil {
		v.Moveunref()
		goto ERR
	}
	//send data over set ok status
	stat.ReStatus = meta.MoveOk
	stat.Utime = time.Now().Unix()
	stat.MoveData = stat.MoveTotalData
	s.zk.SetRecoveryMovestat(vid, deststoreid, &stat)
	v.Moveunref()
	return

ERR:
	stat.ReStatus = meta.MoveFail
	stat.Utime = time.Now().Unix()
	s.zk.SetRecoveryMovestat(vid, deststoreid, &stat)

	log.Errorf("have no this volid %d", vid)
	return
}

// volume recovery recv data req
func (s *Server) recovery_dest(wr http.ResponseWriter, r *http.Request) {
	var (
		vid      int64
		offset   int64
		end      int64
		size     int64
		file     multipart.File
		res      = map[string]interface{}{}
		filetype string
		str      string
		data     []byte
		err      error
	)

	if r.Method != "POST" {
		http.Error(wr, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	defer HttpPostWriter(r, wr, time.Now(), &err, res)

	/*
		if !s.wl.Allow() {
			err = errors.ErrServiceUnavailable
			return
		}
	*/

	str = r.FormValue("mvid")
	if vid, err = strconv.ParseInt(str, 10, 32); err != nil {
		log.Errorf("strconv.ParseInt(\"%s\") error(%v)", str, err)
		err = errors.ErrParam
		return
	}
	filetype = r.FormValue("filetype")
	str = r.FormValue("offset")
	if offset, err = strconv.ParseInt(str, 10, 64); err != nil {
		log.Errorf("strconv.ParseInt(\"%s\") error(%v)", str, err)
		err = errors.ErrParam
		return
	}
	str = r.FormValue("end")
	if end, err = strconv.ParseInt(str, 10, 32); err != nil {
		log.Errorf("strconv.ParseInt(\"%s\") error(%v)", str, err)
		err = errors.ErrParam
		return
	}

	if file, _, err = r.FormFile("file"); err != nil {
		log.Errorf("r.FormFile() error(%v)", err)
		err = errors.ErrInternal
		return
	}

	if size, err = checkFileSize(file, s.conf.NeedleMaxSize); err == nil {
		if size != 0 {
			data, err = s.readalldata(file, size)
			if err != nil {
				err = errors.ErrParam
				return
			}
		}
	} else {
		err = errors.ErrParam
		return
	}

	if err = s.store.recoveryvolume_writedata(int32(vid), filetype, offset, int32(end), data, size); err != nil {
		log.Errorf("do move data failed")
	}

	file.Close()
	return
}

func (s *Store) recoveryvolume_writedata(vid int32, filetype string, offset int64, end int32, data []byte, size int64) (err error) {
	var (
		vol                                      *volumetmp
		find                                     bool
		bdir, idir, bfile, ifile, nbfile, nifile string
		i                                        int
		v, oldvol                                *volume.Volume
	)
	s.rclock.Lock()
	for _, vol = range s.recoveryvolumes {
		if vol.vid == vid {
			find = true
			break
		}
	}
	s.rclock.Unlock()

	if !find {
		// not have move dest volume file add this task
		if vol, err = s.add_recovery_volume(vid); err != nil {
			return
		}
	}

	s.set_movedest_volume_updatetime(vid)

	if len(data) != 0 {
		if filetype == "volume" {
			if _, err = vol.ftvol.WriteAt(data, offset); err != nil {
				log.Errorf("wirte file %s failed error(%v)", vol.file_tmpvolume, err)
				return
			}
		} else {
			if _, err = vol.ftvolindex.WriteAt(data, offset); err != nil {
				log.Errorf("wirte file %s failed error(%v)", vol.file_tmpvolumeindex, err)
				return
			}
		}
	}
	// write volume file and volume index sucess
	if end == 1 && filetype == "volumeindex" {
		bfile, ifile = vol.file_tmpvolume, vol.file_tmpvolumeindex
		bdir, idir = filepath.Dir(bfile), filepath.Dir(ifile)
		for {
			nbfile, nifile = s.file(vid, bdir, idir, i)
			if !myos.Exist(nbfile) && !myos.Exist(nifile) {
				break
			}
			i++
		}

		//move old vol to freevolume
		s.vlock.Lock()
		oldvol = s.Volumes[vid]
		s.vlock.Unlock()

		if oldvol != nil {
			//log.Errorf("destoryvid %d", oldvol.Id)
			oldvol.Close()
			oldvol.Destroy()
			bdir, idir = filepath.Dir(oldvol.Block.File), filepath.Dir(oldvol.Indexer.File)
			_, err = s.AddFreeVolume(1, bdir, idir)
			if err != nil {
				log.Recoverf("recover dest volume add freevolue bdir %s idir %s faild", bdir, idir)
			}
		}

		// rename tmp volume name to volume name
		if err = os.Rename(ifile, nifile); err != nil {
			log.Recoverf(" recover dest volume id %d  os.Rename(\"%s\", \"%s\") error(%v)", vid, ifile, nifile, err)
			return
		}
		if err = os.Rename(bfile, nbfile); err != nil {
			log.Recoverf("recover dest volume id %d os.Rename(\"%s\", \"%s\") error(%v)", vid, bfile, nbfile, err)
			return
		}
		vol.file_tmpvolume, vol.file_tmpvolumeindex = nbfile, nifile
		// add a new volume
		if v, err = newVolume(vid, nbfile, nifile, s.conf); err != nil {
			log.Recoverf("recover dest volume id %d newvolume volume file %s volume index file %s failed err(%v)", vid, nbfile, nifile, err)
			return
		}
		s.vlock.Lock()
		//	oldvol = s.Volumes[vid]
		s.addVolume(vid, v)

		if err = s.saveVolumeIndex(); err != nil {
			s.vlock.Unlock()
			log.Recoverf("recovery add volume: %d error(%v), local index or zookeeper index may save failed", vid, err)
			return
		} else {
			err = s.zk.SetVolume(vid, v.Meta())
			if err != nil {
				log.Recoverf("set zk vid=%d meta=%s", vid, v.Meta())
			}
		}
		s.vlock.Unlock()

		//destory destvolume index file
		s.rclock.Lock()
		defer s.rclock.Unlock()
		if err = s.destoryRecoveryVolumeIndex(vid); err != nil {
			log.Recoverf("detory destvolume index  vid %d failed", vid)
			err = nil
		}

	}
	return

}

/*
//request rebalance stop move volume
func (s *Server) stop_movevolume(wr http.ResponseWriter, r *http.Request) {
	var (
		err         error
		vid         int64
		res         = map[string]interface{}{}
		deststoreid string
	)

	defer HttpPostWriter(r, wr, time.Now(), &err, res)
	if vid, err = strconv.ParseInt(r.FormValue("mvolume"), 10, 32); err != nil {
		log.Errorf("strconv.ParseInt(\"%s\") error(%v)", r.FormValue("vid"), err)
		err = errors.ErrParam
		return
	}

	err = s.store.stop_movevolume(int32(vid))
}

func (s *Store) sendstoptostore(volume *volumerebalance) (err error) {
	var (
		rebalanceapi string
		params       = url.Values{}
		res          meta.DestoryMove
	)
	rebalanceapi, err = s.zk.Getrebalanceapi(volume.deststoreid)
	if err != nil {
		return
	}
	uri = fmt.Sprintf(_destory_move, rebalanceapi)
	params.Set("mvid", fmt.Sprintf("%d", volumerebalance.vid))
	if err = Http("POST", uri, params, nil, &res); err != nil {
		log.Errorf("GET called Http error(%v)", err)
		return
	}
	if res.Ret != errors.RetOK {
		err = errors.ErrInternal
		return
	}
	return
}
*/

// send data sucess, destory volume local index
func (s *Store) destory_movevolumeindex(vid int32) (err error) {
	var (
		mvolume *volumerebalance
		i       int
		flag    = false
	)
	s.mlock.Lock()
	for i, mvolume = range s.rebalancevolumes {
		if mvolume.vid == vid {
			flag = true
			break
		}
	}
	s.mlock.Unlock()
	if !flag {
		return
	}

	/*	mvolume.control <- 0 // 0 is mean stop move volume
		err = s.sendstoptostore(mvolume)
		if err != nil {
			return
		}
	*/
	s.mlock.Lock()
	defer s.mlock.Unlock()

	s.rebalancevolumes = append(s.rebalancevolumes[:i], s.rebalancevolumes[i+1:]...)
	s.saveMoveVolumeIndex()
	return
}

func (s *Store) saveRecoveryVolumeIndex() (err error) {
	var (
		tn, n int
		v     *volumetmp
	)
	if _, err = s.rcvf.Seek(0, os.SEEK_SET); err != nil {
		log.Errorf("mvf.Seek() error(%v)", err)
		return
	}
	for _, v = range s.recoveryvolumes {
		if n, err = s.rcvf.WriteString(fmt.Sprintf("%s,%s,%d\n", v.file_tmpvolume, v.file_tmpvolumeindex, v.vid)); err != nil {
			log.Errorf("mvf.WriteString() error(%v)", err)
			return
		}
		tn += n
	}
	if err = s.rcvf.Sync(); err != nil {
		log.Errorf("mvf.saveMoveVolumeIndex Sync() error(%v)", err)
		return
	}
	if err = os.Truncate(s.conf.Store.RecoveryIndex, int64(tn)); err != nil {
		log.Errorf("os.Truncate() error(%v)", err)
	}
	return
}

func (s *Store) saveMoveDestVolumeIndex() (err error) {
	var (
		tn, n int
		v     *volumetmp
	)
	if _, err = s.dmvf.Seek(0, os.SEEK_SET); err != nil {
		log.Errorf("mvf.Seek() error(%v)", err)
		return
	}
	for _, v = range s.rebalancedestvolumes {
		if n, err = s.dmvf.WriteString(fmt.Sprintf("%s,%s,%d\n", v.file_tmpvolume, v.file_tmpvolumeindex, v.vid)); err != nil {
			log.Errorf("mvf.WriteString() error(%v)", err)
			return
		}
		tn += n
	}
	if err = s.dmvf.Sync(); err != nil {
		log.Errorf("mvf.saveMoveVolumeIndex Sync() error(%v)", err)
		return
	}
	if err = os.Truncate(s.conf.Store.RebalanceDestIndex, int64(tn)); err != nil {
		log.Errorf("os.Truncate() error(%v)", err)
	}
	return
}

// saveFreeVolumeIndex save free volumes index info to disk.
func (s *Store) saveMoveVolumeIndex() (err error) {
	var (
		tn, n int
		v     *volumerebalance
	)
	if _, err = s.mvf.Seek(0, os.SEEK_SET); err != nil {
		log.Errorf("mvf.Seek() error(%v)", err)
		return
	}
	for _, v = range s.rebalancevolumes {
		if n, err = s.mvf.WriteString(fmt.Sprintf("%s,%d,%d\n", v.deststoreid, v.vid, v.newvid)); err != nil {
			log.Errorf("mvf.WriteString() error(%v)", err)
			return
		}
		tn += n
	}
	if err = s.mvf.Sync(); err != nil {
		log.Errorf("mvf.saveMoveVolumeIndex Sync() error(%v)", err)
		return
	}
	if err = os.Truncate(s.conf.Store.RebalanceIndex, int64(tn)); err != nil {
		log.Errorf("os.Truncate() error(%v)", err)
	}
	return
}

//add movevolume task to movevolume list and local rebalancevolumeindex, get a goroute do this movevolume
func (s *Store) addmvvolume(vid, newvid int32, deststoreid string) (err error) {
	var (
		tmp *volumerebalance
	)
	tmp = new(volumerebalance)
	tmp.vid = vid
	tmp.newvid = newvid
	tmp.deststoreid = deststoreid

	s.mlock.Lock()
	defer s.mlock.Unlock()

	s.rebalancevolumes = append(s.rebalancevolumes, tmp)
	err = s.saveMoveVolumeIndex()
	if err != nil {
		return
	}

	go s.movevolume(deststoreid, vid, newvid)
	return
}

func (s *Store) setRebalanceTaskStat(vid int32, stat bool) {
	var (
		mvolume *volumerebalance
	)
	s.mlock.Lock()
	for _, mvolume = range s.rebalancevolumes {
		if mvolume.vid == vid {
			mvolume.status = stat
			break
		}
	}
	s.mlock.Unlock()

}

//goroute do move volume
func (s *Store) movevolume(deststoreid string, movevolid, newvid int32) {
	var (
		rebalanceapi, uri string
		params            = url.Values{}
		res               meta.MoveDataRes
		v                 *volume.Volume
		err1              error
		err               error
		stat              meta.StoreInfo
	)
	t := strings.Split(deststoreid, "/")
	if len(t) < 2 {
		log.Errorf("rack/storeid /")
		return
	}

	stat.Deststoreid = t[1]

	rebalanceapi, err = s.zk.Getrebalanceapi(deststoreid)
	if err != nil {
		return
	}
	uri = fmt.Sprintf(_dest_movedata, rebalanceapi)
	off := strconv.FormatInt(int64(movevolid), 10)
	params.Set("mvid", off)
	//params.Set("mvid", fmt.Sprintf("%d", movevolid))

	if v = s.Volumes[movevolid]; v != nil {
		if v.Moving || v.Compact {
			log.Errorf("have already move ")
			s.setRebalanceTaskStat(movevolid, true)
			return
		}
	} else {
		log.Errorf("have no this volid %d", movevolid)
		s.setRebalanceTaskStat(movevolid, true)
		return
	}
	s.setRebalanceTaskStat(movevolid, false)
	v.Moveref()
	for {
		// read data send, send volume file
		err1 = v.Movevolume(v.Block.File, func(data []byte, offset int64, total int64, end bool) (err error) {
			off := strconv.FormatInt(offset, 10)
			params.Set("offset", off)
			params.Set("filetype", "volume")
			if end {
				params.Set("end", "1")
			} else {
				params.Set("end", "0")
			}
			for {
				if err = Http("POST", uri, params, data, &res); err != nil {
					log.Errorf("GET called Http uri %s error(%v)", uri, err)
				}
				if res.Ret != errors.RetOK || err != nil {
					if err == nil {
						err = errors.ErrInternal
						log.Errorf("send volume %s faild (%v)", v.Block.File, err)
					}
					stat.MoveStatus = meta.MoveFail
					stat.UTime = time.Now().Unix()
					s.zk.SetVolMovestat(movevolid, &stat)
					time.Sleep(100 * time.Millisecond)

					continue
				}
				stat.MoveStatus = meta.Moving
				stat.MoveTotalData = total
				stat.MoveData = offset
				stat.UTime = time.Now().Unix()

				s.zk.SetVolMovestat(movevolid, &stat)
				break
			}
			return
		})

		if err1 != nil {
			stat.MoveStatus = meta.MoveFail
			stat.UTime = time.Now().Unix()
			s.zk.SetVolMovestat(movevolid, &stat)
			time.Sleep(100 * time.Millisecond)
			continue
		}
	resend:
		// send volume index data
		err1 = v.Movevolume(v.Indexer.File, func(data []byte, offset int64, total int64, end bool) (err error) {
			off := strconv.FormatInt(offset, 10)
			params.Set("offset", off)
			params.Set("filetype", "volumeindex")
			if end {
				params.Set("end", "1")
			} else {
				params.Set("end", "0")
			}
			for {
				if err = Http("POST", uri, params, data, &res); err != nil {
					log.Errorf("GET called Http uri %s error(%v)", uri, err)

				}
				if res.Ret != errors.RetOK || err != nil {
					if err != nil {
						err = errors.ErrInternal
						log.Errorf("send volume %s faild (%v)", v.Indexer.File, err)
					}

					stat.MoveStatus = meta.MoveFail
					stat.UTime = time.Now().Unix()
					s.zk.SetVolMovestat(movevolid, &stat)
					time.Sleep(100 * time.Millisecond)
					continue
				}

				stat.MoveStatus = meta.Moving
				//stat.MoveTotalData = total
				//stat.MoveData = offset
				stat.UTime = time.Now().Unix()

				s.zk.SetVolMovestat(movevolid, &stat)
				break

			}
			return
		})
		if err1 != nil {
			stat.MoveStatus = meta.MoveFail
			stat.UTime = time.Now().Unix()
			s.zk.SetVolMovestat(movevolid, &stat)
			time.Sleep(100 * time.Millisecond)
			log.Errorf("have no this volid %d", movevolid)
			goto resend
		}

		break
	}
	v.Moveunref()
	//send data complete and destory old volume
	if err = s.destory_rebalanceindex(movevolid, newvid); err != nil {
		log.Recoverf("rebalance volume %d destory volume faild", movevolid)
	}
	stat.MoveStatus = meta.MoveOk
	stat.UTime = time.Now().Unix()
	stat.MoveData = stat.MoveTotalData
	err = s.zk.SetVolMovestat(movevolid, &stat)
	if err != nil {
		log.Recoverf("rebalance volume %d update sucess failed", movevolid)
	}
	log.Infof("volume %d move sucess", movevolid)

}

// destory old volume and  add a free volume
func (s *Store) destoryvolume(vid int32) (err error) {
	var (
		bdir string
		idir string
		v    *volume.Volume
	)

	if v = s.Volumes[vid]; v != nil {

	} else {
		log.Errorf("have no this volid %d", vid)
		return
	}
	bdir, idir = filepath.Dir(v.Block.File), filepath.Dir(v.Indexer.File)
	// destory volume
	v.Destroy()

	// delete volume
	if err = s.DelVolume(vid); err != nil {
		log.Errorf("del volume  %d failed", vid)
		return
	}

	//add a new freevolume
	_, err = s.AddFreeVolume(1, bdir, idir)
	if err != nil {
		log.Errorf("delete volume id %d add freevolume bdir %s idir %s failed", bdir, idir)
	}
	return
}

// send data sucess ,do destory old volume
func (s *Store) destory_rebalanceindex(vid, newvid int32) (err error) {

	// destory old volume
	if err = s.destoryvolume(vid); err != nil {
		log.Recoverf("rebalance volume destory /rack/store/vid %d failed", vid)
		return
	}
	//del zookeeper /volume/vid/storeid
	if err = s.zk.DelVolumeStore(vid); err != nil {
		log.Recoverf("rebalance delete zk volume id %d storeid failed", vid)
		return
	}
	//destory movevolumeindex
	if err = s.destory_movevolumeindex(vid); err != nil {
		log.Recoverf("rebalance volume destory movvolume id %d index faild", vid)
		return
	}
	//add new volume
	if _, err = s.AddVolume(newvid); err != nil {
		log.Recoverf("rebalance volume add new volume vid %d failed", vid)
		return
	}
	return

}
