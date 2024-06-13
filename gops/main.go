package main

import (
	"efs/gops/models/ops"
	_ "efs/gops/routers"
	"github.com/astaxie/beego"
)

func main() {
	//beego.LoadAppConfig("ini", "conf/gops.conf")
	ops.InitOps()
	beego.Run()
}
