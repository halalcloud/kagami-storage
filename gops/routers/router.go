package routers

import (
	"efs/gops/controllers"

	"github.com/astaxie/beego"
)

func init() {

	beego.Router("/", &controllers.MainController{})
	beego.Router("/login", &controllers.ApiController{}, "get,post:Login")
	//overview
	beego.Router("/overview", &controllers.ApiController{}, "get,post:OverView")
	beego.Router("/stat", &controllers.ApiController{}, "get,post:StatOverView")
	//rack
	beego.Router("/rack", &controllers.ApiController{}, "get:GetRack")
	beego.Router("/rstore", &controllers.ApiController{}, "get:GetRackStore")
	beego.Router("/rsqps", &controllers.ApiController{}, "get:GetStoreQps")
	beego.Router("/rstp", &controllers.ApiController{}, "get:GetStoreTp")
	beego.Router("/rsdelay", &controllers.ApiController{}, "get:GetStoreDelay")
	beego.Router("/rsdevice", &controllers.ApiController{}, "get:GetStoreDevice")
	beego.Router("/addfreevolume", &controllers.ApiController{}, "get,post:AddFreeVolume")
	//group
	beego.Router("/group", &controllers.ApiController{}, "get:GetGroup")
	beego.Router("/freestore", &controllers.ApiController{}, "get:GetFreeStore")
	beego.Router("/addgroup", &controllers.ApiController{}, "get,post:AddGroup")
	beego.Router("/delgroup", &controllers.ApiController{}, "get:DelGroup")
	beego.Router("/addvolume", &controllers.ApiController{}, "get,post:AddVolume")
	beego.Router("/volume", &controllers.ApiController{}, "get:GetVolume")
	beego.Router("/compactvolume", &controllers.ApiController{}, "get:CompactVolume")
	//recover
	beego.Router("/recovervolume", &controllers.ApiController{}, "get:RecoverVolume")
	beego.Router("/recovervolumestore", &controllers.ApiController{}, "get:RecoverVolumeStore")
	beego.Router("/recoverstatus", &controllers.ApiController{}, "get:RecoverStatus")
	//rebalance
	beego.Router("/rebalancestart", &controllers.ApiController{}, "get:RebalanceStart")
	beego.Router("/rebalancestatus", &controllers.ApiController{}, "get,post:RebalanceStatus")
	beego.Router("/rebalancefinish", &controllers.ApiController{}, "get:RebalanceFinish")
	//oplog
	beego.Router("/oplog", &controllers.ApiController{}, "get:GetOpLog")
	//almrec
	beego.Router("/almrec", &controllers.ApiController{}, "get:GetAlmRec")
	//user
	beego.Router("/user", &controllers.ApiController{}, "get:GetUser")
	beego.Router("/adduser", &controllers.ApiController{}, "get,post:AddUser")
	beego.Router("/deluser", &controllers.ApiController{}, "get,post:DelUser")
	beego.Router("/activateuser", &controllers.ApiController{}, "get,post:ActivateUser")
	beego.Router("/resetpasswd", &controllers.ApiController{}, "get,post:ResetUserPasswd")
	beego.Router("/modifypasswd", &controllers.ApiController{}, "get,post:ModifyPasswd")
	beego.Router("/userbyid", &controllers.ApiController{}, "get:GetUserbyId")
	beego.Router("/resetuser", &controllers.ApiController{}, "get,post:ResetUser")
	beego.Router("/modifyuser", &controllers.ApiController{}, "get,post:ModifyUser")

}
