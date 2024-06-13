package main

import (
	"efs/libs/errors"
	"efs/libs/meta"
	"efs/libs/stat"
	"efs/store/volume"
	"encoding/json"
	"fmt"
	"net/http"
	"os/exec"
	"strings"
	"time"

	log "efs/log/glog"
)

const (
	statDuration = 1 * time.Second
)

func (s *Server) startStat() {
	var (
		err      error
		serveMux = http.NewServeMux()
		server   = &http.Server{
			Addr:    s.conf.StatListen,
			Handler: serveMux,
			// TODO read/write timeout
		}
	)
	s.info = &stat.Info{
		Ver:       Ver,
		GitSHA1:   GitSHA1,
		StartTime: time.Now(),
		Stats:     &stat.Stats{},
	}
	go s.statproc()
	serveMux.HandleFunc("/info", s.stat)
	serveMux.HandleFunc("/diskinfo", s.diskinfo)
	serveMux.HandleFunc("/getvolumeinfo", s.getvolumeinfo)
	if err = server.Serve(s.statSvr); err != nil {
		log.Errorf("server.Serve() error(%v)", err)
	}
	log.Info("http stat stop")
	return
}

func (s *Server) stat(wr http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(wr, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	var (
		err     error
		data    []byte
		v       *volume.Volume
		volumes = make([]*volume.Volume, 0, len(s.store.Volumes))
		res     = map[string]interface{}{"ret": errors.RetOK}
	)
	for _, v = range s.store.Volumes {
		volumes = append(volumes, v)
	}
	res["server"] = s.info
	res["volumes"] = volumes
	//res["free_volumes"] = s.store.FreeVolumes
	res["free_volumes"] = len(s.store.FreeVolumes)
	if data, err = json.Marshal(res); err == nil {
		if _, err = wr.Write(data); err != nil {
			log.Errorf("wr.Write() error(%v)", err)
		}
	} else {
		log.Errorf("json.Marshal() error(%v)", err)
	}
	return
}

func (s *Server) getlocaldisk() (disk_arry []*stat.Diskinfo, err error) {
	var (
		tmpinfo *stat.Diskinfo
		flag    int
	)
	dfcmd := "sh " + s.conf.DfshellPath

	disk_arry = make([]*stat.Diskinfo, 0)
	cmd := exec.Command("/bin/bash", "-c", dfcmd)
	out, err := cmd.CombinedOutput()
	if err != nil {
		log.Errorf("get disk do shell error(%v)", err)
	}
	outstring := string(out)

	info := strings.Split(outstring, "&")
	for i, tmp := range info {
		if tmp == "" {
			continue
		}
		if len(tmp) > 1 && tmp == "\n" {
			continue
		}
		if len(tmp) > 3 && tmp[:4] == "/dev" {
			tmpinfo = new(stat.Diskinfo)
			tmpinfo.Devname = tmp
			flag = i
		}
		if i == (flag + 1) {
			tmpinfo.Total_space = tmp
		}
		if i == (flag + 2) {
			tmpinfo.Avail_space = tmp
		}
		if i == (flag + 3) {
			if tmp == "/" {
				continue
			}
			tmpinfo.Mountpoint = tmp
			disk_arry = append(disk_arry, tmpinfo)
		}

	}
	return
}

func (s *Server) diskinfo(wr http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(wr, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	var (
		err       error
		data      []byte
		res       = map[string]interface{}{"ret": errors.RetOK}
		disk_info []*stat.Diskinfo
	)

	disk_info, err = s.getlocaldisk()
	if err != nil {
		log.Errorf("get local disk info error(%v)", err)
	}
	res["diskinfo"] = disk_info
	if data, err = json.Marshal(res); err == nil {
		if _, err = wr.Write(data); err != nil {
			log.Errorf("wr.Write() error(%v)", err)
		}
	} else {
		log.Errorf("json.Marshal() error(%v)", err)
	}
	return
}

func (s *Server) getvolumeinfo(wr http.ResponseWriter, r *http.Request) {
	var (
		fv      []*volume.Volume
		volumes map[int32]*volume.Volume

		diskfvolume map[string][]*volume.Volume
		diskvolume  map[string][]*volume.Volume
		vol         *volume.Volume
		vols        []*volume.Volume
		tmp         []string
		dir         string

		volinfos meta.VolInfoResponse
		vinfo    *meta.VolInfo
		data     []byte
		ok       bool
		err      error
	)

	if r.Method != "GET" {
		http.Error(wr, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	fv = s.store.FreeVolumes
	volumes = s.store.Volumes

	diskvolume = make(map[string][]*volume.Volume)
	diskfvolume = make(map[string][]*volume.Volume)
	for _, vol = range volumes {
		t := strings.Split(vol.Block.File, "/")
		if len(t) < 2 {
			log.Errorf("volume block file path not /")
			return
		}

		dir = t[1]
		//	dir = filepath.Dir(vol.Block.File)
		if vols, ok = diskvolume[dir]; ok {
			vols = append(vols, vol)
			diskvolume[dir] = vols
		} else {
			diskvolume[dir] = make([]*volume.Volume, 0)
			diskvolume[dir] = append(diskvolume[dir], vol)
		}
	}

	for _, vol = range fv {
		t := strings.Split(vol.Block.File, "/")
		if len(t) < 2 {
			log.Errorf("volume block file path not /")
			return
		}

		dir = t[1]
		if vols, ok = diskfvolume[dir]; ok {
			vols = append(vols, vol)
			diskfvolume[dir] = vols
		} else {
			diskfvolume[dir] = make([]*volume.Volume, 0)
			diskfvolume[dir] = append(diskfvolume[dir], vol)
		}
	}

	for dir, vols = range diskfvolume {
		vinfo = new(meta.VolInfo)
		vinfo.Dirpath = dir
		for _, vol = range vols {

			vinfo.Freevids = append(vinfo.Freevids, "-1")
		}
		if vols, ok = diskvolume[dir]; ok {
			for _, vol = range vols {
				vinfo.Vids = append(vinfo.Vids, fmt.Sprintf("%d", vol.Id))
			}
		}

		nums := len(vinfo.Freevids) - s.conf.ReservedFreevolumePerDisk
		if nums < 0 || nums == 0 {
			vinfo.Freevids = tmp
		} else {
			vinfo.Freevids = vinfo.Freevids[:nums]
		}
		volinfos.VolMes = append(volinfos.VolMes, vinfo)
	}
	if data, err = json.Marshal(volinfos); err == nil {
		if _, err = wr.Write(data); err != nil {
			log.Errorf("wr.Write() error(%v)", err)
		}
	} else {
		log.Errorf("json.Marshal() error(%v)", err)
	}
	return

}

// statproc stat the store.
func (s *Server) statproc() {
	var (
		v    *volume.Volume
		olds *stat.Stats
		news = new(stat.Stats)
	)
	for {
		olds = s.info.Stats
		*news = *olds
		s.info.Stats = news // use news instead, for current display
		olds.Reset()
		for _, v = range s.store.Volumes {
			v.Stats.Calc()
			olds.Merge(v.Stats)
		}
		olds.Calc()
		s.info.Stats = olds
		time.Sleep(statDuration)
	}
}
