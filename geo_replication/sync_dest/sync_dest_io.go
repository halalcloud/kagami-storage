package sync_dest

import (
	"fmt"
	"io"
	"kagamistoreage/geo_replication/conf"
	log "kagamistoreage/log/glog"
	"os"
	"path/filepath"
)

type Dest_io struct {
	c *conf.Config
}

type Dest_file_fd struct {
	Dfile *os.File
}

func checkFileIsExist(filename string) bool {
	var exist = true
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		exist = false
	}
	return exist
}

func New(c *conf.Config) (dio *Dest_io, err error) {
	dio = &Dest_io{}
	dio.c = c
	return
}

func (d *Dest_io) Getfilefd(file string) (dfile *Dest_file_fd, err error) {
	dfile = &Dest_file_fd{}
	fullfile := d.c.RootPath + "/" + file
	fpath := filepath.Dir(fullfile)

	if !checkFileIsExist(fpath) {
		err = os.MkdirAll(fpath, 0777)
		if err != nil {
			log.Errorf("mkdirall fpath failed %v", fpath)
			return
		}
	}
	if checkFileIsExist(fullfile) {
		dfile.Dfile, err = os.OpenFile(fullfile, os.O_RDWR|os.O_TRUNC, 0666)
		if err != nil {
			log.Errorf("open file %s failed %v", fullfile, err)
			return
		}
	} else {
		dfile.Dfile, err = os.Create(fullfile)
		if err != nil {
			log.Errorf("create file %s failed %v", fullfile, err)
			return
		}
	}
	return

}

func (d *Dest_io) File_delete(filename string) (err error) {

	fullfile := d.c.RootPath + "/" + filename
	err = os.Remove(fullfile)
	if err != nil {
		log.Errorf("remove filename %s failed %v", fullfile, err)
	}
	return
}

func (d *Dest_io) Del_dir(deldir string) (err error) {

	dir := d.c.RootPath + "/" + deldir
	err = os.RemoveAll(dir)
	if err != nil {
		log.Errorf("removeall dir %s failed %v", dir, err)
	}
	return
}

func (d *Dest_io) File_copy(src, dest string) (err error) {
	var (
		srcfile, destfile *os.File
	)
	srcfullfile := d.c.RootPath + "/" + src

	srcfile, err = os.OpenFile(srcfullfile, os.O_RDWR, 0666)
	if err != nil {
		log.Errorf("open file %s failed %v", srcfullfile, err)
		return
	}
	defer srcfile.Close()

	destfullfile := d.c.RootPath + "/" + dest
	fpath := filepath.Dir(destfullfile)

	if !checkFileIsExist(fpath) {
		err = os.MkdirAll(fpath, 0777)
		if err != nil {
			log.Errorf("mkdirall fpath failed %v", fpath)
			return
		}
	}
	destfile, err = os.OpenFile(destfullfile, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		log.Errorf("open file %s failed %v", destfullfile, err)
		return
	}
	defer destfile.Close()

	_, err = io.Copy(destfile, srcfile)
	if err != nil {
		log.Errorf("copy srcfilename %s  destfilename %s failed %v", srcfullfile, destfullfile, err)
	}
	return
}

func (d *Dest_io) File_mv(src, dest string) (err error) {
	srcfullfile := d.c.RootPath + "/" + src
	destfullfile := d.c.RootPath + "/" + dest

	err = os.Rename(srcfullfile, destfullfile)
	if err != nil {
		log.Errorf("rename srcfilename %s  destfilename %s failed %v", srcfullfile, destfullfile, err)
	}
	return
}

func (df *Dest_file_fd) Writedata(data []byte) (err error) {
	var (
		n, tmp, datalen int
	)
	datalen = len(data)
	fmt.Println("write data len %d", datalen)
	for {
		if tmp == datalen {
			break
		}
		n, err = df.Dfile.WriteString(string(data[tmp:]))
		if err != nil {
			log.Errorf("write data failed %v", err)
			return
		}
		if n != len(data[tmp:]) {
			tmp = tmp + n
		} else {
			break
		}
	}
	return

}

func (df *Dest_file_fd) Close() {
	df.Dfile.Close()
}
