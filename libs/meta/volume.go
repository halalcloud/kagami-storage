package meta

import "kagamistoreage/libs/stat"

type Volume struct {
	Id          int32       `json:"id"`
	Del_numbers int32       `json:"Del_numbers"`
	Moving      bool        `json:"moving"`
	Damage      bool        `json:"damage"`
	Compact     bool        `json:"compact"`
	Block       *SuperBlock `json:"block"`
	Stats       *stat.Stats `json:"stats"`
}

type Volumes struct {
	Volumes []*Volume `json:"volumes"`
}

// VolumeState  for zk /volume stat
type VolumeState struct {
	TotalWriteProcessed uint64 `json:"total_write_processed"`
	TotalWriteDelay     uint64 `json:"total_write_delay"`
	FreeSpace           uint32 `json:"free_space"`
}
