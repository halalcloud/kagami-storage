/*
Navicat MySQL Data Transfer

Source Server         : 192.168.200.80
Source Server Version : 50634
Source Host           : 192.168.200.80:3306
Source Database       : gops

Target Server Type    : MYSQL
Target Server Version : 50634
File Encoding         : 65001

Date: 2017-04-05 10:16:45
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for almrec
-- ----------------------------
DROP TABLE IF EXISTS `almrec`;
CREATE TABLE `almrec` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '主键ID',
  `alarm_detail` varchar(250) NOT NULL COMMENT '报警信息',
  `receive_uid` varchar(250) NOT NULL COMMENT '警报接收用户',
  `create_time` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for oplog
-- ----------------------------
DROP TABLE IF EXISTS `oplog`;
CREATE TABLE `oplog` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '主键ID',
  `op_time` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00' COMMENT '操作时间',
  `op_uid` int(11) NOT NULL COMMENT '操作用户UID',
  `op_detail` varchar(250) DEFAULT NULL COMMENT '操作细节',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=236 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for store_delay
-- ----------------------------
DROP TABLE IF EXISTS `store_delay`;
CREATE TABLE `store_delay` (
  `id` bigint(11) NOT NULL AUTO_INCREMENT,
  `ctime` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `storeid` varchar(150) NOT NULL,
  `upload` int(11) NOT NULL,
  `download` int(11) NOT NULL,
  `del` int(11) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1000887 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for store_qps
-- ----------------------------
DROP TABLE IF EXISTS `store_qps`;
CREATE TABLE `store_qps` (
  `id` bigint(11) NOT NULL AUTO_INCREMENT,
  `ctime` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `storeid` varchar(150) NOT NULL,
  `upload` int(11) NOT NULL,
  `download` int(11) NOT NULL,
  `del` int(11) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1000911 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for throughput
-- ----------------------------
DROP TABLE IF EXISTS `throughput`;
CREATE TABLE `throughput` (
  `id` bigint(11) NOT NULL AUTO_INCREMENT,
  `ctime` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `storeid` varchar(150) NOT NULL,
  `tpin` int(11) NOT NULL,
  `tpout` int(11) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1000883 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for user
-- ----------------------------
DROP TABLE IF EXISTS `user`;
CREATE TABLE `user` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '主键ID',
  `acount` varchar(100) NOT NULL COMMENT '账户名',
  `password` varchar(100) NOT NULL COMMENT '密码',
  `name` varchar(100) NOT NULL COMMENT '姓名',
  `role` tinyint(4) NOT NULL DEFAULT '2' COMMENT '用户角色（1：超级用户，2普通用户）',
  `stat` tinyint(4) NOT NULL DEFAULT '1' COMMENT '用户状态（1：启用，2：禁用）',
  `is_alarm` tinyint(4) NOT NULL DEFAULT '2' COMMENT '是否接收警告（1：接受，2：不接受）',
  `mail` varchar(50) NOT NULL COMMENT '邮箱',
  `phone` varchar(50) DEFAULT NULL COMMENT '手机号',
  `qq` varchar(50) DEFAULT NULL COMMENT 'QQ号码',
  `remark` varchar(250) DEFAULT NULL COMMENT '备注',
  `last_login` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00' COMMENT '最后登入时间',
  `create_time` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00' COMMENT '创建时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `acount_unique` (`acount`),
  UNIQUE KEY `mail_unique` (`mail`),
  UNIQUE KEY `phone_unique` (`phone`),
  UNIQUE KEY `qq_unique` (`qq`)
) ENGINE=InnoDB AUTO_INCREMENT=167 DEFAULT CHARSET=utf8;
