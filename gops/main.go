package main

import (
	"kagamistoreage/gops/models/ops"
	_ "kagamistoreage/gops/routers"

	"github.com/astaxie/beego"
)

func main() {
	//beego.LoadAppConfig("ini", "conf/gops.conf")
	ops.InitOps()
	beego.Run()
}
