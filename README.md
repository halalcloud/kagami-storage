efs
==============
`efs` 是基于facebook haystack 用golang实现的小文件存储系统。

---------------------------------------
  * [特性](#特性)
  * [安装](#安装)
  * [集群](#集群)
  * [API](#API)
  * [更多](#更多)

---------------------------------------

## 特性
 * 高吞吐量和低延迟
 * 容错性
 * 高效
 * 维护简单

## 安装

### 一、安装hbase、zookeeper

 * 参考hbase官网. 安装、启动请查看[这里](https://hbase.apache.org/).
 * 参考zookeeper官网. 安装、启动请查看[这里](http://zookeeper.apache.org/).

### 二、搭建golang、python环境

 * 参考golang官网. 安装请查看[这里](https://golang.org/doc/install).
 * 参考python官网. 安装请查看[这里]
(https://www.python.org/)

### 三、安装gosnowflake

 * 参考[这里](https://github.com/Terry-Mao/gosnowflake)

### 四、部署
1.下载efs及依赖包
```sh
$ mkdir /ecloud/src -p
$ cd /ecloud
$ export GOPATH=$GOPATH:`pwd`
$ cd src
$ git clone git@git.c4hcdn.cn:cloud-storage-group/efs.git
$ cd efs
$ go get ./...
```

2.安装directory、store、pitchfork、proxy模块(配置文件请依据实际机器环境配置)
```sh
$ cd /ecloud/src/efs/directory
$ go install
$ cp directory.toml /ecloud/bin/directory.toml
$ cd ../store/
$ go install
$ cp store.toml /ecloud/bin/store.toml
$ cd ../pitchfork/
$ go install
$ cp pitchfork.toml /ecloud/bin/pitchfork.toml
$ cd ../proxy
$ go install
$ cp proxy.toml /ecloud/bin/proxy.toml

```
到此所有的环境都搭建完成！

### 五、启动
```sh
$ cd /ecloud/bin
$ nohup /ecloud/bin/directory -c /ecloud/bin/directory.toml &
$ nohup /ecloud/bin/store -c /ecloud/bin/store.toml &
$ nohup /ecloud/bin/pitchfork -c /ecloud/bin/pitchfork.toml &
$ nohup /ecloud/bin/proxy -c /ecloud/bin/proxy.toml &
$ cd /ecloud/src/efs/ops
$ nohup python runserver.py &
```

### 六、测试
 * efs初始化，分配存储空间，请查看[这里](http://git.c4hcdn.cn:85/cloud-storage-group/efs/tree/master/ops/README.md)
 * 请求efs，请查看[这里](http://git.c4hcdn.cn:85/cloud-storage-group/efs/tree/master/doc/proxy.md)

## 集群

![Aaron Swartz](doc/efs_server.png?raw=true)

### directory

 * directory主要负责请求的均匀调度和元数据管理，元数据存放在hbase，由gosnowflake产生文件key

### store

 * store主要负责文件的物理存储

### pitchfork

 * pitchfork负责监控store的服务状态、可用性和磁盘状态

### proxy

 * proxy作为efs存储的代理以及维护bucket相关

### ops

 * ops作为efs的后台管理界面，负责分配存储、扩容、压缩等维护工作
 
## API
[api文档](http://git.c4hcdn.cn:85/cloud-storage-group/efs/tree/master/doc)

## 更多

 * [efs-image-server](https://github.com/jackminicloud/efs-image-server) 
test
