package multipartupload

import (
	"kagamistoreage/authacess/conf"

	"io"
	"kagamistoreage/libs/errors"
	log "kagamistoreage/log/glog"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Uploadpart struct {
	Partid    string //key id
	Hash      string
	Vid       int32
	Offset    int64
	Timeout   int64
	Blocksize int64
	partfile  *os.File
}

type Multipart struct {
	Uploadparts map[string]*Uploadpart
	c           *conf.Config
}

var (
	Uploadparts map[string]*Uploadpart
	plock       *sync.RWMutex
)

func Multipart_init(c *conf.Config) (multipart *Multipart, err error) {
	multipart = &Multipart{}
	multipart.c = c
	Uploadparts = make(map[string]*Uploadpart)
	multipart.Uploadparts = Uploadparts
	plock = &sync.RWMutex{}
	err = init_multipart(multipart)
	if err != nil {
		log.Errorf("init upload part tmp file failed %v", err)
		return
	}
	go Timeout_clean(multipart)
	return
}

func init_multipart(multipart *Multipart) (err error) {
	var (
		partpath string
	)

	partpath = multipart.c.PartTmppath
	err = filepath.Walk(partpath, lisfunc)
	return

}

func lisfunc(path string, f os.FileInfo, err error) error {
	if f == nil {
		return err
	}
	if f.IsDir() {
		return nil
	}
	part := new(Uploadpart)
	part.Partid = f.Name()
	part.Offset = int64(f.Size())
	part.Timeout = f.ModTime().Unix()
	part.partfile, err = os.OpenFile(path, os.O_RDWR, 0755)
	if err != nil {
		return err
	}

	Uploadparts[part.Partid] = part
	return nil

}

func (p *Uploadpart) Destorypart(path string) {
	dfilepath := path + "/" + p.Partid
	err := os.Remove(dfilepath)
	if err != nil {
		log.Errorf("filename %s timeout remove failed %v", dfilepath, err)
	}
	_ = p.partfile.Close()
}

func Timeout_clean(multipart *Multipart) {
	for {
		var destorylist []string
		plock.RLock()
		for file, part := range multipart.Uploadparts {
			if (time.Now().Unix() - part.Timeout) > multipart.c.MultipartDataTimeout {
				destorylist = append(destorylist, file)
			}
		}
		plock.RUnlock()

		for _, file := range destorylist {
			plock.RLock()
			part := multipart.Uploadparts[file]
			plock.RUnlock()
			part.Destorypart(multipart.c.PartTmppath)
			plock.Lock()
			delete(multipart.Uploadparts, file)
			plock.Unlock()
		}
		time.Sleep(multipart.c.DataCleanTimeout.Duration)
	}
}

func (m *Multipart) Regist_partid(partid string, blocksize int64, data []byte) (err error) {
	var (
		partname string
		n, wlen  int
	)

	part := new(Uploadpart)
	part.Partid = partid
	part.Blocksize = blocksize
	part.Timeout = time.Now().Unix()
	partname = m.c.PartTmppath + "/" + partid
	part.partfile, err = os.Create(partname)
	if err != nil {
		log.Errorf("create partfile %s failed %v", partname, err)
		return
	}
	tlen := len(data)
	for {
		if wlen == tlen {
			break
		}
		n, err = part.partfile.WriteAt(data[wlen:], int64(wlen))
		if n != len(data[wlen:]) && err != nil {
			log.Errorf("write file %s data failed %v", partname, err)
			return
		}
		wlen += n

	}

	part.Offset += int64(tlen)
	plock.Lock()
	m.Uploadparts[part.Partid] = part
	plock.Unlock()
	return
}

func (m *Multipart) Put_partid(partid string, offset int64, data []byte) (resoffset, blocksize int64, err error) {
	var (
		part      *Uploadpart
		ok        bool
		n, wlen   int
		tmpoffset int64
	)
	plock.RLock()
	part, ok = m.Uploadparts[partid]
	plock.RUnlock()
	if !ok {
		log.Errorf("have no this partid %d", partid)
		err = errors.ErrHavenoBlockId
		return
	}
	if part.Offset != 0 && part.Offset != offset {
		log.Errorf("offset %d is not match last offset %d ", offset, part.Offset)
		err = errors.ErrOffsetNotMatch
		return
	}
	tlen := len(data)
	tmpoffset = offset
	for {
		if wlen == tlen {
			break
		}
		n, err = part.partfile.WriteAt(data[wlen:], tmpoffset)
		if n != len(data[wlen:]) && err != nil {
			log.Errorf("write file %s data failed %v", partid, err)
			return
		}
		wlen += n
		tmpoffset += int64(n)

	}

	part.Offset = tmpoffset
	resoffset = tmpoffset
	blocksize = part.Blocksize
	return

}

func (m *Multipart) Getdata_partid(partid string) (data []byte, err error) {
	var (
		tdata             []byte
		part              *Uploadpart
		ok                bool
		readsize, n, rlen int
		pos               int64
	)
	plock.RLock()
	part, ok = m.Uploadparts[partid]
	plock.RUnlock()
	if !ok {
		log.Errorf("have no this partid %d", partid)
		err = errors.ErrHavenoBlockId
		return
	}
	readsize = int(part.Offset)
	if part.Offset == 0 {
		readsize = int(m.c.SliceFileSize)
	}
	tdata = make([]byte, readsize)
	for {
		n, err = part.partfile.ReadAt(tdata[rlen:], pos)
		if err != nil && err != io.EOF {
			log.Errorf("read partid %s failed %v", partid, err)
			return
		}
		if n == 0 {
			break
		}
		rlen += n
		pos += int64(n)
	}
	data = tdata
	return

}

func (m *Multipart) Destory_partid(partid string) (err error) {
	var (
		part *Uploadpart
		ok   bool
	)
	plock.RLock()
	part, ok = m.Uploadparts[partid]
	plock.RUnlock()
	if !ok {
		log.Errorf("have no this partid %d", partid)
		err = errors.ErrHavenoBlockId
		return
	}
	part.Destorypart(m.c.PartTmppath)
	plock.Lock()
	delete(m.Uploadparts, partid)
	plock.Unlock()
	return
}

func (m *Multipart) Is_exsit_partid(partid string) (exsit bool) {
	plock.RLock()
	_, exsit = m.Uploadparts[partid]
	plock.RUnlock()
	return
}

func (m *Multipart) Partid_back_offset(partid string, back_len int64) {
	plock.RLock()
	part, ok := m.Uploadparts[partid]
	plock.RUnlock()
	if ok {
		part.Offset = part.Offset - back_len
	}

}
