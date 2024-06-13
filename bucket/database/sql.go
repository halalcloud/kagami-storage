package database

import (
	"database/sql"
	"efs/bucket/conf"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	log "efs/log/glog"
)

var (
	Db *sql.DB
)

func New(c *conf.Config) (err error) {
	var err1 error
	Db, err1 = sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8", c.DatabaseUname, c.Databasepasswd, c.DatabaseAddr, c.DatabasePort, c.DatabaseName))
	if err1 != nil {
		return
	}

	if err1 := Db.Ping(); err1 != nil {
		err = err1
		log.Errorf("error ping database: %s", err.Error())
		return
	}
	return
}
