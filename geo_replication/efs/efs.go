package efs

import (
	"kagamistoreage/geo_replication/conf"
)

type Src_io struct {
	c *conf.Config
}

func New(c *conf.Config) (src *Src_io, err error) {
	src = &Src_io{}
	src.c = c
	return
}
