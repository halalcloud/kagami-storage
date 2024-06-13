package meta

type StopRblFailed struct {
	Code  int    `json:"code"`
	Error string `json:"error"`
}
type FinishRblFailed struct {
	Code  int    `json:"code"`
	Error string `json:"error"`
}

type VidValue struct {
	DestVid     string
	DestRockId  map[string]string
	DestStoreId []string
	DestGroupId string
}

type StoreInfo struct {
	MoveStatus    int
	MoveTotalData int64
	MoveData      int64
	UTime         int64
	Deststoreid   string
}

type VidInfo struct {
	Vidinfo    VidValue
	Storesinfo map[string]StoreInfo
}

type GroupSizeInfo struct {
	VolumeCount     int
	FreeVolumeCount int
	PutCount        int
	PopCount        int
}

type VolInfoResponse struct {
	VolMes []*VolInfo `json:"volmes"`
}

type Destorymove struct {
	Ret int `json:"ret"`
}

type MoveStatus struct {
	MoveStatus    int   `json:"movestatus"`
	MoveTotalData int64 `json:"movetotaldata"`
	MoveData      int64 `json:"movedata"`
	Utime         int64 `json:"utime"`
}

type MoveDataRes struct {
	Ret int `json:"ret"`
}
