package database

import (
	"database/sql"
	"fmt"
	"kagamistoreage/bucket/conf"

	log "kagamistoreage/log/glog"

	_ "github.com/go-sql-driver/mysql"
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
