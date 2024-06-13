package main

//bucket create request
type bcreate_req struct {
	Region      string `json:"region"`
	Imgsource   string `json:"imgsource"`
	Key         string `json:"key"`
	Keysecret   string `json:"keysecret"`
	Dnsname     string `json:"dnsname,omitempty"`
	Userdnsname string `json:"userdnsname,omitempty"`
	Propety     int64  `json:"propety"`
	Replication int    `json:"replication"`
	Userid      int    `json:"userid"`
}

//request efs bucket create
type bc_req struct {
	Families string `json:"families"`
}

type error_res struct {
	Error string `json:"error"`
	Code  int    `json:"code"`
}

type Propety struct {
	Property int64 `json:"propety"`
}

type Imagesource struct {
	Imgsource string `json:"imgsource"`
}

type AkSkInfo struct {
	Acesskey  string `json:"acesskey"`
	Secertkey string `json:"secertkey"`
}

type StyleDelimiter struct {
	Delimiters []string `json:"delimiters"`
}

type DPStyle struct {
	Style map[string]string `json:"style"`
}

type bget_res struct {
	Bucketname     string `json:"bucketname"`
	Region         string `json:"region"`
	Imgsource      string `json:"imgsource"`
	Key            string `json:"key"`
	Keysecret      string `json:"keysecret"`
	Propety        int64  `json:"propety"`
	Dnsname        string `json:"dnsname"`
	Userdnsname    string `json:"userdnsname"`
	Replication    int    `json:"replication"`
	StyleDelimiter string `json:"styledelimiter"`
	DPStyle        string `json:"dpstyle"`
	Uid            int    `json:"uid"`
	Ctime          string `json:"ctime"`
}
