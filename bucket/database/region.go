package database

import (
	_ "database/sql"
	"errors"

	log "kagamistoreage/log/glog"

	_ "github.com/go-sql-driver/mysql"
)

func AddRegion(region string) (id int64, err error) {
	var id_tmp int64
	stmt, err1 := Db.Prepare("INSERT region SET region_name=?")
	if err1 != nil {
		log.Errorf("add region prepare database: %s", err1.Error())
		err = err1
		return
	}

	rs, err1 := stmt.Exec(region)
	if err1 != nil {
		err = err1
		log.Errorf("add region exe database: %s", err1.Error())
		return
	}
	id_tmp, err = rs.LastInsertId()
	if err1 != nil {
		log.Errorf("add region exe database: %s", err1.Error())
		err = err1
	}
	id = id_tmp
	return
}

func DelRegion(region string) (err error) {
	stmt, err1 := Db.Prepare("DELETE FROM `region` WHERE region=?")
	if err1 != nil {
		log.Errorf(" delete region prepare database: %s", err.Error())
		err = err1
		return
	}

	if _, err = stmt.Exec(region); err != nil {
		log.Errorf("delete region exe database: %s", err.Error())
	}
	return

}

func GetRegionname(id int) (rname string, err error) {
	var (
		tid int
	)
	if err1 := Db.QueryRow("SELECT * FROM region WHERE id=?", id).Scan(&tid, &rname); err1 != nil {
		log.Errorf("get region from region id database: %s", err1.Error())
		err = errors.New("database get region from region id failed")
		return
	}
	return
}

func GetRegionid(rname string) (id int, err error) {
	var (
		tname string
	)
	if err1 := Db.QueryRow("SELECT * FROM region WHERE region_name=?", rname).Scan(&id, &tname); err1 != nil {
		log.Errorf("get region from region name database: %s", err1.Error())
		err = errors.New("database get region from region name failed")
		return
	}
	return
}

func GetRegion() (regions map[int64]string, err error) {
	var reg map[int64]string
	var id int64
	var tmp string

	reg = make(map[int64]string)
	rows, err1 := Db.Query("SELECT * FROM region")
	if err1 != nil {
		log.Errorf("update propety prepare database: %s", err.Error())
		err = err1
		return
	}
	defer rows.Close()
	for rows.Next() {
		if err = rows.Scan(&id, &tmp); err != nil {
			log.Errorf("get regions exe database: %s", err.Error())
			return
		}
		reg[int64(id)] = tmp
	}
	regions = reg
	return
}
