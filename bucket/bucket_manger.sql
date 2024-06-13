/*
Navicat MySQL Data Transfer

Source Server         : efsonline
Source Server Version : 50720
Source Host           : 172.18.210.37:3306
Source Database       : bucket_manger

Target Server Type    : MYSQL
Target Server Version : 50720
File Encoding         : 65001

Date: 2018-04-26 16:06:11
*/

SET FOREIGN_KEY_CHECKS=0;
-- ----------------------------
-- Table structure for `bucket`
-- ----------------------------
DROP TABLE IF EXISTS `bucket`;
CREATE TABLE `bucket` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `userid` int(11) NOT NULL,
  `bucket_name` text NOT NULL COMMENT 'bucket name',
  `region_id` int(11) NOT NULL COMMENT 'region id',
  `image_source` text NOT NULL COMMENT 'image source',
  `keyid` text NOT NULL,
  `propety` int(11) NOT NULL,
  `dnsname` text NOT NULL COMMENT 'bucket dns name',
  `userdnsname` text,
  `keysecret` text NOT NULL,
  `replication` int(11) NOT NULL,
  `style_delimiter` varchar(255) DEFAULT '',
  `dpstyle` varchar(2000) DEFAULT '',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'create time',
  PRIMARY KEY (`id`)
) ENGINE=MyISAM AUTO_INCREMENT=208 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of bucket
-- ----------------------------
INSERT INTO bucket VALUES ('1', '11', 'test', '2', 'http://aaaaac.bucket.efs.gsgslb.com', 'QhYK8_SyVdgwe_Iq9-tCsu7QtgWyr9Mw', '0', 'aaaaab.bucket.efs.gsgslb.com', null, 'OKK5Sn6PlV7Xyl4nLT8IsqG4Y-AVoj7G', '2', '', '', '2017-10-23 20:29:13');
INSERT INTO bucket VALUES ('9', '11', 'avatar', '2', 'http://baidu.com/aaa', 'QhYK8_SyVdgwe_Iq9-tCsu7QtgWyr9Mw', '0', 'aaaaaj.bucket.efs.gsgslb.com', 'avatar', 'OKK5Sn6PlV7Xyl4nLT8IsqG4Y-AVoj7G', '2', '', '{\"test1\":\"imageView2\\/1\\/w\\/200\\/h\\/200\\/q\\/75|imageslim\",\"png\":\"imageView2\\/0\\/q\\/75\\/format\\/PNG\",\"jpg\":\"imageView2\\/2\\/w\\/200\\/h\\/200\\/q\\/75\\/format\\/JPG\"}', '2017-10-31 10:17:59');
INSERT INTO bucket VALUES ('49', '31', 'v0.bjadks.com', '2', 'imgsource', 'DakkEuml9UDaiZyvKdNn3YUwr7_Q4vw1', '0', '5bgzacz.bucket.efs.gsgslb.com', 'v0.bjadks.com', 'NTc8behRhkjJGQJeKCIDGt0CRoVLloVQ', '2', '', '', '2018-01-04 14:10:49');
INSERT INTO bucket VALUES ('4', '14', 'renyang-bak1.com', '2', 'imgsource', 'YkcuE1GU-_jVICAz3EQNx6kFzmYOf55k', '0', 'aaaaae.bucket.efs.gsgslb.com', null, 'NMb8xdEt_cpiBgv-pAS4lY_2sQR_VJIS', '1', '', '', '2017-10-24 14:19:30');
INSERT INTO bucket VALUES ('5', '1', 'tanshiping', '2', 'imgsource', 'VpXcYY9AV4whboeC8TMj-JxleMeLSdEg', '0', 'aaaaaf.bucket.efs.gsgslb.com', 'tanshiping', 'LBdYob8uqPVGDpuI2zThx2e2Rnsly755', '1', '', '', '2017-10-24 14:30:53');
INSERT INTO bucket VALUES ('6', '14', 'renyang-bak2.com', '2', 'imgsource', 'YkcuE1GU-_jVICAz3EQNx6kFzmYOf55k', '0', 'aaaaag.bucket.efs.gsgslb.com', null, 'NMb8xdEt_cpiBgv-pAS4lY_2sQR_VJIS', '2', '', '', '2017-10-24 14:34:28');
INSERT INTO bucket VALUES ('7', '14', 'renyang-bak3.com', '2', 'imgsource', 'YkcuE1GU-_jVICAz3EQNx6kFzmYOf55k', '0', 'aaaaah.bucket.efs.gsgslb.com', null, 'NMb8xdEt_cpiBgv-pAS4lY_2sQR_VJIS', '3', '', '', '2017-10-24 14:35:10');
INSERT INTO bucket VALUES ('17', '17', 'example', '2', 'imgsource', 'NqDaST8-W3JASAg_KzX3tjnVAagaLJy7', '0', 'aaaaar.bucket.efs.gsgslb.com', null, '08wI0-2zR4IAOEzQ6GeXKTBnaecKl2ZV', '1', '', '', '2017-11-17 14:23:11');
INSERT INTO bucket VALUES ('13', '0', 'example', '2', 'imgsource', 'aaaaaaaaaaaaaa', '0', 'aaaaan.bucket.efs.gsgslb.com', null, 'bbbbbbbbbbbbbb', '1', '', '', '2017-11-13 15:53:59');
INSERT INTO bucket VALUES ('15', '14', 'test1.com', '2', 'imgsource', 'YkcuE1GU-_jVICAz3EQNx6kFzmYOf55k', '0', 'aaaaap.bucket.efs.gsgslb.com', null, 'NMb8xdEt_cpiBgv-pAS4lY_2sQR_VJIS', '1', '', '', '2017-11-15 11:13:53');
INSERT INTO bucket VALUES ('19', '4', 'php-test', '2', 'imgsource', 'Lcqx8mVqUe2BT27ZViBaDoBTD2KDCdx9', '0', 'aaaaat.bucket.efs.gsgslb.com', null, 'JNBPKuphok-F8e5pxe18ZqAI7o8fPug4', '2', '', '', '2017-11-20 16:28:25');
INSERT INTO bucket VALUES ('36', '1', 'test3.com', '2', 'imgsource', 'VpXcYY9AV4whboeC8TMj-JxleMeLSdEg', '0', 'bqfm2az.bucket.efs.gsgslb.com', 'test3.com', 'LBdYob8uqPVGDpuI2zThx2e2Rnsly755', '1', '', '', '2017-12-11 15:18:38');
INSERT INTO bucket VALUES ('39', '27', 'sjk-bucket', '2', 'imgsource', '29Zo32sIAtHN4RxBTMbR-gpXw_MbIPh7', '1', '1p679az.bucket.efs.gsgslb.com', 'sjk-bucket', 'voqrGGJky5_ri847728nkb4qewZlwg36', '1', '', '', '2017-12-15 17:48:15');
INSERT INTO bucket VALUES ('41', '6', 'test', '2', 'imgsource', 'LtE4EgrqoufqAWbsZ6LdYHtulUfSkSJB', '0', 'g16xibz.bucket.efs.gsgslb.com', 'test', 'uu8-DUOqF_sqTwg5YgEjaRlLZh2m6HWi', '1', '', '', '2017-12-20 10:50:51');
INSERT INTO bucket VALUES ('171', '15', 'download.esnai.net', '2', 'imgsource', 'H9rmUZEoKgqnF2JFaZNO3wq69dm9tRPZ', '0', 'pfsvncz.bucket.efs.gsgslb.com', 'download.esnai.net', '_Mmc23sIzrf08nx88T5Zy4Q7OLm-tq_n', '2', '', '', '2018-01-11 13:20:29');
INSERT INTO bucket VALUES ('51', '31', 'v1.bjadks.com', '2', 'imgsource', 'DakkEuml9UDaiZyvKdNn3YUwr7_Q4vw1', '0', '56z2acz.bucket.efs.gsgslb.com', 'v1.bjadks.com', 'NTc8behRhkjJGQJeKCIDGt0CRoVLloVQ', '2', '', '', '2018-01-04 15:27:32');
INSERT INTO bucket VALUES ('53', '41', 'vn.bgaldk.com', '2', 'imgsource', 'n5tUNQTJ5azQjJVoxbrer8_XJYWXuAa3', '1', 'fbbh3acz.bucket.efs.gsgslb.com', 'vn.bgaldk.com', 'y80t_81Dyv5LxI2_H_d44Fgs8F3PVRt_', '1', '', '', '2018-01-04 15:37:49');
INSERT INTO bucket VALUES ('55', '43', 'gs.dangbei.net', '2', 'imgsource', 'EgGPwZ6G0VIQKf1gvyi1UZpKUvDMm7Em', '0', 'hbha4acz.bucket.efs.gsgslb.com', 'gs.dangbei.net', 'lYdLv49busIVHsq0rfeNK1Y8ea8pNPsL', '2', '', '', '2018-01-04 15:55:19');
INSERT INTO bucket VALUES ('57', '45', 'downvip2.smartgame-down.com', '2', 'imgsource', '1t_7f3vu0LJK-K_zMBnqkU-BqeREo3Hi', '0', 'jbqd6acz.bucket.efs.gsgslb.com', 'downvip2.smartgame-down.com', '7C73u-a2DqplqxjOZSZBy7yktho2NU7d', '2', '', '', '2018-01-04 16:40:28');
INSERT INTO bucket VALUES ('59', '45', 'g2.androidgame-store.com', '2', 'imgsource', '1t_7f3vu0LJK-K_zMBnqkU-BqeREo3Hi', '0', 'jb0d6acz.bucket.efs.gsgslb.com', 'g2.androidgame-store.com', '7C73u-a2DqplqxjOZSZBy7yktho2NU7d', '2', '', '', '2018-01-04 16:40:38');
INSERT INTO bucket VALUES ('61', '55', 'baasvedio.tt286.com', '2', 'imgsource', 'yUpbDULOkbwhUZTpupZcRo8t8YroAWeY', '0', 'tbzt8acz.bucket.efs.gsgslb.com', 'baasvedio.tt286.com', 'grPm-L5FT9lhhtSS56AOI2TVzimml6Ie', '2', '', '', '2018-01-04 17:33:25');
INSERT INTO bucket VALUES ('63', '55', 'egoures.tt286.com', '2', 'imgsource', 'yUpbDULOkbwhUZTpupZcRo8t8YroAWeY', '0', 'tbsu8acz.bucket.efs.gsgslb.com', 'egoures.tt286.com', 'grPm-L5FT9lhhtSS56AOI2TVzimml6Ie', '2', '', '', '2018-01-04 17:33:54');
INSERT INTO bucket VALUES ('65', '55', 'newmarket1.oo523.com', '2', 'imgsource', 'yUpbDULOkbwhUZTpupZcRo8t8YroAWeY', '0', 'tb3u8acz.bucket.efs.gsgslb.com', 'newmarket1.oo523.com', 'grPm-L5FT9lhhtSS56AOI2TVzimml6Ie', '1', '', '', '2018-01-04 17:34:05');
INSERT INTO bucket VALUES ('67', '55', 'newmarket.oo523.com', '2', 'imgsource', 'yUpbDULOkbwhUZTpupZcRo8t8YroAWeY', '0', 'tbcv8acz.bucket.efs.gsgslb.com', 'newmarket.oo523.com', 'grPm-L5FT9lhhtSS56AOI2TVzimml6Ie', '1', '', '', '2018-01-04 17:34:14');
INSERT INTO bucket VALUES ('69', '55', 'newmarket.tt286.com', '2', 'imgsource', 'yUpbDULOkbwhUZTpupZcRo8t8YroAWeY', '0', 'tbsv8acz.bucket.efs.gsgslb.com', 'newmarket.tt286.com', 'grPm-L5FT9lhhtSS56AOI2TVzimml6Ie', '1', '', '', '2018-01-04 17:34:30');
INSERT INTO bucket VALUES ('71', '55', 'otaretest.dd351.com', '2', 'imgsource', 'yUpbDULOkbwhUZTpupZcRo8t8YroAWeY', '0', 'tbbw8acz.bucket.efs.gsgslb.com', 'otaretest.dd351.com', 'grPm-L5FT9lhhtSS56AOI2TVzimml6Ie', '2', '', '', '2018-01-04 17:34:49');
INSERT INTO bucket VALUES ('73', '53', 'mediacdn.good321.net', '2', 'imgsource', 'vpMHHDhDq0Ez65J3QINu0zyAwKANovx7', '0', 'rbndbbcz.bucket.efs.gsgslb.com', 'mediacdn.good321.net', 'TADI2_YYR-fxYndxEWXsS_PC4S0JFhEA', '2', '', '', '2018-01-04 18:28:25');
INSERT INTO bucket VALUES ('75', '49', 'download-gs.idol001.com', '2', 'imgsource', 'DU_SE6Q9m4g4iYvU8BQu0iHldQDF4aXE', '0', 'nb34bbcz.bucket.efs.gsgslb.com', 'download-gs.idol001.com', 'CF6Te0h7de3keaUTodgk7V-WRvtcn8E3', '2', '', '', '2018-01-04 18:44:53');
INSERT INTO bucket VALUES ('77', '51', 'download.shininghunter.com', '2', 'imgsource', 'oPEo7DFS6nBntU8ak3_imu9L0zgCWxHR', '0', 'pbnucbcz.bucket.efs.gsgslb.com', 'download.shininghunter.com', 'IXLUKTRpxnSNFG5kW2NRC_cvN_4r-6wq', '2', '', '', '2018-01-04 19:00:13');
INSERT INTO bucket VALUES ('79', '57', 'download.rshui.cn', '2', 'imgsource', 'iOkcjejn42QFVJyrH3moaQZhFefcJ6fH', '0', 'vbl9cbcz.bucket.efs.gsgslb.com', 'download.rshui.cn', 'soeClGCZ5GmD5aV25FSxTIt68qs65OR1', '2', '', '', '2018-01-04 19:09:11');
INSERT INTO bucket VALUES ('81', '57', 'download.sthero.com', '2', 'imgsource', 'iOkcjejn42QFVJyrH3moaQZhFefcJ6fH', '0', 'vbv9cbcz.bucket.efs.gsgslb.com', 'download.sthero.com', 'soeClGCZ5GmD5aV25FSxTIt68qs65OR1', '2', '', '', '2018-01-04 19:09:21');
INSERT INTO bucket VALUES ('83', '57', 'fhdz.rshui.cn', '2', 'imgsource', 'iOkcjejn42QFVJyrH3moaQZhFefcJ6fH', '0', 'vb59cbcz.bucket.efs.gsgslb.com', 'fhdz.rshui.cn', 'soeClGCZ5GmD5aV25FSxTIt68qs65OR1', '2', '', '', '2018-01-04 19:09:31');
INSERT INTO bucket VALUES ('85', '57', 'update.rshui.cn', '2', 'imgsource', 'iOkcjejn42QFVJyrH3moaQZhFefcJ6fH', '0', 'vbdadbcz.bucket.efs.gsgslb.com', 'update.rshui.cn', 'soeClGCZ5GmD5aV25FSxTIt68qs65OR1', '2', '', '', '2018-01-04 19:09:39');
INSERT INTO bucket VALUES ('87', '57', 'update.sthero.com', '2', 'imgsource', 'iOkcjejn42QFVJyrH3moaQZhFefcJ6fH', '0', 'vbtadbcz.bucket.efs.gsgslb.com', 'update.sthero.com', 'soeClGCZ5GmD5aV25FSxTIt68qs65OR1', '2', '', '', '2018-01-04 19:09:55');
INSERT INTO bucket VALUES ('89', '59', 'download.m818.com', '2', 'imgsource', 'qANCIsy6kr7pW4KFrVVqxPL-UXiiwWsO', '0', 'xb3vjccz.bucket.efs.gsgslb.com', 'download.m818.com', 'B3Ha76wEbWKZnkbmRC08ifbWow4iy9sd', '2', '', '', '2018-01-05 10:29:53');
INSERT INTO bucket VALUES ('91', '61', 'download.funshion.com', '2', 'imgsource', 'qAdNJp6ewGWv0rvNfsf0xLrBvoZ1QlaC', '0', 'zbwmkccz.bucket.efs.gsgslb.com', 'download.funshion.com', 'HGoQ9M0zDN49EvYimMOf0RpRejVA5wiJ', '2', '', '', '2018-01-05 10:45:58');
INSERT INTO bucket VALUES ('93', '63', 'mp4.alisports.com', '2', 'imgsource', 'MG6cykeKRVmSmYjJBVTvkBmTjBoHXaFb', '0', '1b6ukccz.bucket.efs.gsgslb.com', 'mp4.alisports.com', 'SEhBtzTOWcAVLFJGCEaKKBvdLjkuWlSR', '2', '', '', '2018-01-05 10:50:56');
INSERT INTO bucket VALUES ('95', '97', 'gstest.aginomoto.com', '2', 'imgsource', 'OMvZ90rTjgl9Z0xeSIITFfEIi4XXRPY6', '0', 'zc1z1ccz.bucket.efs.gsgslb.com', 'gstest.aginomoto.com', 'mGYuE8TT58zA97YS2wldsBlMYap520oT', '2', '', '', '2018-01-05 17:01:03');
INSERT INTO bucket VALUES ('97', '69', 'gs.video.changbashow.com', '2', 'imgsource', 'nEfNgcnmXPgkeNUfV37Zw2CnfZHVLxnL', '0', '7bczeicz.bucket.efs.gsgslb.com', 'gs.video.changbashow.com', 'PSRlGd9f221UcPzRvA-ddFCt-OJ_GtCV', '2', '', '', '2018-01-08 14:29:26');
INSERT INTO bucket VALUES ('99', '49', 'videoplay-gs.idol001.com', '2', 'imgsource', 'DU_SE6Q9m4g4iYvU8BQu0iHldQDF4aXE', '0', 'nbxpficz.bucket.efs.gsgslb.com', 'videoplay-gs.idol001.com', 'CF6Te0h7de3keaUTodgk7V-WRvtcn8E3', '2', '', '', '2018-01-08 14:45:23');
INSERT INTO bucket VALUES ('101', '77', 'gsvod.love.tv', '2', 'imgsource', 'PTrUY7m3toGJG_gjPIinLEKqXCJd9xwi', '0', 'fc36ficz.bucket.efs.gsgslb.com', 'gsvod.love.tv', '9WviexaJdk4GV3JSC96T_Wllx7xGUq82', '2', '', '', '2018-01-08 14:55:41');
INSERT INTO bucket VALUES ('103', '83', 'mediags.mango.aginomoto.com', '2', 'imgsource', '5vnxydb_rT1j9dGh8PQxmxZlxkytuUXy', '0', 'lcpdhicz.bucket.efs.gsgslb.com', 'mediags.mango.aginomoto.com', '57gwJRnqkEwFA02G3S-b9UIu4Qfk4EXL', '2', '', '', '2018-01-08 15:21:15');
INSERT INTO bucket VALUES ('105', '89', 'gs.vod.qixun.tv', '2', 'imgsource', 'k-gHgyaGISbER11nep4nvRBYoUK8sW4g', '0', 'rceaiicz.bucket.efs.gsgslb.com', 'gs.vod.qixun.tv', 'rbsnEYJ7ZxQjuOqoRz7kcBsQIk6BJePE', '2', '', '', '2018-01-08 15:40:52');
INSERT INTO bucket VALUES ('107', '103', 'mediags.download.vr.moguv.com', '2', 'imgsource', '_V4JdvuIO3-B7cDcvnzCRbhwrZTJUcv0', '0', '5c7olicz.bucket.efs.gsgslb.com', 'mediags.download.vr.moguv.com', 'OmCgCZx0wwTklRSgS_3XKxpdRmSvO72y', '2', '', '', '2018-01-08 16:54:33');
INSERT INTO bucket VALUES ('109', '69', 'gs.video.changbalive.com', '2', 'imgsource', 'nEfNgcnmXPgkeNUfV37Zw2CnfZHVLxnL', '0', '7be4jkcz.bucket.efs.gsgslb.com', 'gs.video.changbalive.com', 'PSRlGd9f221UcPzRvA-ddFCt-OJ_GtCV', '2', '', '', '2018-01-09 18:15:40');
INSERT INTO bucket VALUES ('111', '69', 'senhua-6-102.live.changba.com', '2', 'imgsource', 'nEfNgcnmXPgkeNUfV37Zw2CnfZHVLxnL', '0', '7bs6jkcz.bucket.efs.gsgslb.com', 'senhua-6-102.live.changba.com', 'PSRlGd9f221UcPzRvA-ddFCt-OJ_GtCV', '2', '', '', '2018-01-09 18:17:06');
INSERT INTO bucket VALUES ('113', '69', 'x103.live.changba.com', '2', 'imgsource', 'nEfNgcnmXPgkeNUfV37Zw2CnfZHVLxnL', '0', '7b57jkcz.bucket.efs.gsgslb.com', 'x103.live.changba.com', 'PSRlGd9f221UcPzRvA-ddFCt-OJ_GtCV', '2', '', '', '2018-01-09 18:17:55');
INSERT INTO bucket VALUES ('115', '75', 'camvod302.iqilu.com', '2', 'imgsource', 'eApyTRVH5SI9eHYruaPoQa03EDxYh0T_', '0', 'dcehlkcz.bucket.efs.gsgslb.com', 'camvod302.iqilu.com', 'dMRZtvOXhRDXhLy6q9dh_UdTjTvncCoa', '2', '', '', '2018-01-09 18:45:04');
INSERT INTO bucket VALUES ('117', '75', 'flashvodgs302.iqilu.com', '2', 'imgsource', 'eApyTRVH5SI9eHYruaPoQa03EDxYh0T_', '0', 'dcvhlkcz.bucket.efs.gsgslb.com', 'flashvodgs302.iqilu.com', 'dMRZtvOXhRDXhLy6q9dh_UdTjTvncCoa', '2', '', '', '2018-01-09 18:45:21');
INSERT INTO bucket VALUES ('119', '75', 'vodgs302.iqilu.com', '2', 'imgsource', 'eApyTRVH5SI9eHYruaPoQa03EDxYh0T_', '0', 'dchilkcz.bucket.efs.gsgslb.com', 'vodgs302.iqilu.com', 'dMRZtvOXhRDXhLy6q9dh_UdTjTvncCoa', '2', '', '', '2018-01-09 18:45:43');
INSERT INTO bucket VALUES ('121', '77', 'gspull.lofficiel.cn', '2', 'imgsource', 'PTrUY7m3toGJG_gjPIinLEKqXCJd9xwi', '0', 'fcwllkcz.bucket.efs.gsgslb.com', 'gspull.lofficiel.cn', '9WviexaJdk4GV3JSC96T_Wllx7xGUq82', '2', '', '', '2018-01-09 18:47:46');
INSERT INTO bucket VALUES ('123', '77', 'gsvod.lofficiel.cn', '2', 'imgsource', 'PTrUY7m3toGJG_gjPIinLEKqXCJd9xwi', '0', 'fcamlkcz.bucket.efs.gsgslb.com', 'gsvod.lofficiel.cn', '9WviexaJdk4GV3JSC96T_Wllx7xGUq82', '2', '', '', '2018-01-09 18:48:00');
INSERT INTO bucket VALUES ('125', '79', 'vod.visiondk.com', '2', 'imgsource', 'aahTW2sVgd10RKZHO49Yxru-HdW0rynl', '0', 'hc08lkcz.bucket.efs.gsgslb.com', 'vod.visiondk.com', '1h9axnqJn6oY9MJeVMy6B84DRgPtSAUC', '2', '', '', '2018-01-09 19:01:38');
INSERT INTO bucket VALUES ('127', '83', 'vadgs.moguv.com', '2', 'imgsource', '5vnxydb_rT1j9dGh8PQxmxZlxkytuUXy', '0', 'lc9bmkcz.bucket.efs.gsgslb.com', 'vadgs.moguv.com', '57gwJRnqkEwFA02G3S-b9UIu4Qfk4EXL', '2', '', '', '2018-01-09 19:03:35');
INSERT INTO bucket VALUES ('129', '83', 'mediags.ottvideowj.hifuntv.com', '2', 'imgsource', '5vnxydb_rT1j9dGh8PQxmxZlxkytuUXy', '0', 'lctcmkcz.bucket.efs.gsgslb.com', 'mediags.ottvideowj.hifuntv.com', '57gwJRnqkEwFA02G3S-b9UIu4Qfk4EXL', '2', '', '', '2018-01-09 19:03:55');
INSERT INTO bucket VALUES ('131', '83', 'mediags.moguv.com', '2', 'imgsource', '5vnxydb_rT1j9dGh8PQxmxZlxkytuUXy', '0', 'lc8dmkcz.bucket.efs.gsgslb.com', 'mediags.moguv.com', '57gwJRnqkEwFA02G3S-b9UIu4Qfk4EXL', '2', '', '', '2018-01-09 19:04:46');
INSERT INTO bucket VALUES ('133', '83', 'mediags.csl.moguv.com', '2', 'imgsource', '5vnxydb_rT1j9dGh8PQxmxZlxkytuUXy', '0', 'lc6emkcz.bucket.efs.gsgslb.com', 'mediags.csl.moguv.com', '57gwJRnqkEwFA02G3S-b9UIu4Qfk4EXL', '2', '', '', '2018-01-09 19:05:20');
INSERT INTO bucket VALUES ('135', '83', 'mediags666.moguv.com', '2', 'imgsource', '5vnxydb_rT1j9dGh8PQxmxZlxkytuUXy', '0', 'lcrfmkcz.bucket.efs.gsgslb.com', 'mediags666.moguv.com', '57gwJRnqkEwFA02G3S-b9UIu4Qfk4EXL', '1', '', '', '2018-01-09 19:05:41');
INSERT INTO bucket VALUES ('137', '83', 'mediags666.mogu', '2', 'imgsource', '5vnxydb_rT1j9dGh8PQxmxZlxkytuUXy', '0', 'lcfgmkcz.bucket.efs.gsgslb.com', 'mediags666.mogu', '57gwJRnqkEwFA02G3S-b9UIu4Qfk4EXL', '2', '', '', '2018-01-09 19:06:05');
INSERT INTO bucket VALUES ('139', '65', 'app1.3dov.cn', '2', 'imgsource', '-TMEmT8n1Lx7zt2Cf1OM-UTJDpHeFkBZ', '0', '3bhnwlcz.bucket.efs.gsgslb.com', 'app1.3dov.cn', 'kdrpFeTbgEAmDmLtKHs9iev5s4mHCLri', '2', '', '', '2018-01-10 11:43:55');
INSERT INTO bucket VALUES ('141', '67', 'downvideo.51tv.c4hcdn.cn', '2', 'imgsource', 'C8K5GrLaBkzjvGKmy_1QI5cpCYJt5WGe', '0', '5b9owlcz.bucket.efs.gsgslb.com', 'downvideo.51tv.c4hcdn.cn', 'X1_uC3SnWpxSAvz8wLMaoJCp66gnHCUQ', '2', '', '', '2018-01-10 11:44:59');
INSERT INTO bucket VALUES ('143', '67', 'longvideo.51tv.c4hcdn.cn', '2', 'imgsource', 'C8K5GrLaBkzjvGKmy_1QI5cpCYJt5WGe', '0', '5bwpwlcz.bucket.efs.gsgslb.com', 'longvideo.51tv.c4hcdn.cn', 'X1_uC3SnWpxSAvz8wLMaoJCp66gnHCUQ', '2', '', '', '2018-01-10 11:45:22');
INSERT INTO bucket VALUES ('145', '71', 'live.haoyishu.org', '2', 'imgsource', 'XdjflP6p3ybayniIfqG56GdVAudpDBwU', '0', '9bbzwlcz.bucket.efs.gsgslb.com', 'live.haoyishu.org', 'YNrE429HqiroONroRhSnwjYxcoDFgLts', '2', '', '', '2018-01-10 11:51:01');
INSERT INTO bucket VALUES ('147', '73', 'raw.media.zuoyoupk.com', '2', 'imgsource', 'v1iQzzrv052yNjmj5BSHqDF43qKE6jIS', '0', 'bcy3wlcz.bucket.efs.gsgslb.com', 'raw.media.zuoyoupk.com', 'kitHuSVhae4s09pwbNwSSRCsAVnhqb-H', '2', '', '', '2018-01-10 11:53:48');
INSERT INTO bucket VALUES ('149', '89', 'gsvod.qixun.tv', '2', 'imgsource', 'k-gHgyaGISbER11nep4nvRBYoUK8sW4g', '0', 'rcilxlcz.bucket.efs.gsgslb.com', 'gsvod.qixun.tv', 'rbsnEYJ7ZxQjuOqoRz7kcBsQIk6BJePE', '2', '', '', '2018-01-10 12:04:20');
INSERT INTO bucket VALUES ('151', '89', 'hls.live.qixun.tv', '2', 'imgsource', 'k-gHgyaGISbER11nep4nvRBYoUK8sW4g', '0', 'rc1lxlcz.bucket.efs.gsgslb.com', 'hls.live.qixun.tv', 'rbsnEYJ7ZxQjuOqoRz7kcBsQIk6BJePE', '2', '', '', '2018-01-10 12:04:39');
INSERT INTO bucket VALUES ('153', '59', 'mp4.snh48.com', '2', 'imgsource', 'qANCIsy6kr7pW4KFrVVqxPL-UXiiwWsO', '0', 'xbtk2lcz.bucket.efs.gsgslb.com', 'mp4.snh48.com', 'B3Ha76wEbWKZnkbmRC08ifbWow4iy9sd', '2', '', '', '2018-01-10 13:51:55');
INSERT INTO bucket VALUES ('155', '59', 'mp5.snh48.com', '2', 'imgsource', 'qANCIsy6kr7pW4KFrVVqxPL-UXiiwWsO', '0', 'xbzl2lcz.bucket.efs.gsgslb.com', 'mp5.snh48.com', 'B3Ha76wEbWKZnkbmRC08ifbWow4iy9sd', '2', '', '', '2018-01-10 13:52:37');
INSERT INTO bucket VALUES ('157', '59', 'ts.snh48.com', '2', 'imgsource', 'qANCIsy6kr7pW4KFrVVqxPL-UXiiwWsO', '0', 'xbim2lcz.bucket.efs.gsgslb.com', 'ts.snh48.com', 'B3Ha76wEbWKZnkbmRC08ifbWow4iy9sd', '2', '', '', '2018-01-10 13:52:56');
INSERT INTO bucket VALUES ('159', '87', 'video-gs.qiaqia.tv', '2', 'imgsource', 'R5oSbXk1gaVNPFLEJFgCEsHXbtjUrb0n', '0', 'pc5p4lcz.bucket.efs.gsgslb.com', 'video-gs.qiaqia.tv', 'tsNpHVjzZndT5XoMKObEXYtyxIBYIaGE', '2', '', '', '2018-01-10 14:38:19');
INSERT INTO bucket VALUES ('161', '101', 'buffer.yuanjing.tv', '2', 'imgsource', 'WY9TSZlZzKF4r_b9C5UdzAK6yZT0JXZg', '0', '3c8u4lcz.bucket.efs.gsgslb.com', 'buffer.yuanjing.tv', 'qoKmX3aJIbB9dwGar6K9-oEl3MdLz77u', '2', '', '', '2018-01-10 14:41:22');
INSERT INTO bucket VALUES ('163', '105', 'vod.wcareer.cn', '2', 'imgsource', 'ZbqNNiXLdf_nS0ObUnwym63wTXj5OcOI', '0', '7cy76lcz.bucket.efs.gsgslb.com', 'vod.wcareer.cn', 'Qw7-avEotOWR8GN4pTas5JQ-5Ndg6PMW', '2', '', '', '2018-01-10 15:32:12');
INSERT INTO bucket VALUES ('165', '93', 'cdncache.vyanke.com', '2', 'imgsource', 'L9r8trxGsWcA25pxO_92Y8F2WYuMHEFh', '0', 'vcowfmcz.bucket.efs.gsgslb.com', 'cdncache.vyanke.com', 'E1lqBWk_GTuMW7GvuginBBiS8LjzZuQi', '2', '', '', '2018-01-10 18:39:50');
INSERT INTO bucket VALUES ('167', '93', 'www.vyanke.com', '2', 'imgsource', 'L9r8trxGsWcA25pxO_92Y8F2WYuMHEFh', '0', 'vc1wfmcz.bucket.efs.gsgslb.com', 'www.vyanke.com', 'E1lqBWk_GTuMW7GvuginBBiS8LjzZuQi', '2', '', '', '2018-01-10 18:40:03');
INSERT INTO bucket VALUES ('169', '93', 'yi.vkeplus.com', '2', 'imgsource', 'L9r8trxGsWcA25pxO_92Y8F2WYuMHEFh', '0', 'vc7wfmcz.bucket.efs.gsgslb.com', 'yi.vkeplus.com', 'E1lqBWk_GTuMW7GvuginBBiS8LjzZuQi', '2', '', '', '2018-01-10 18:40:09');
INSERT INTO bucket VALUES ('173', '95', 'cdn.wehelpu.cn', '2', 'imgsource', 'qtw6KCFaRwE7zLYUwgTU07qABeU7Lw6C', '0', 'xc3d9ncz.bucket.efs.gsgslb.com', 'cdn.wehelpu.cn', 'ptXhwtxKrXAjhNz99F2vnmzUDPAIYuQF', '2', '', '', '2018-01-11 18:14:17');
INSERT INTO bucket VALUES ('175', '95', 'pricdn.wehelpu.cn', '2', 'imgsource', 'qtw6KCFaRwE7zLYUwgTU07qABeU7Lw6C', '0', 'xcde9ncz.bucket.efs.gsgslb.com', 'pricdn.wehelpu.cn', 'ptXhwtxKrXAjhNz99F2vnmzUDPAIYuQF', '2', '', '', '2018-01-11 18:14:27');
INSERT INTO bucket VALUES ('177', '81', 'mediatest.gosun.com', '2', 'imgsource', 'OH2PQBb3h2SHzJS5wcQF_8-ff6FfVRX_', '0', 'jcuz9ncz.bucket.efs.gsgslb.com', 'mediatest.gosun.com', 'PfS_Q3PO7uFqRatiHi-8OWeUGJpauOcx', '2', '', '', '2018-01-11 18:27:20');
INSERT INTO bucket VALUES ('179', '91', 'video.test.dnsgslb.c4hcdn.com', '2', 'imgsource', 'k22W-RXMajahZi9cryGL9sZspq_uFtYf', '0', 'tc9xaocz.bucket.efs.gsgslb.com', 'video.test.dnsgslb.c4hcdn.com', 'I8PrfSGfLubSvh11MexrRFTLn_4FWRMb', '2', '', '', '2018-01-11 18:47:59');
INSERT INTO bucket VALUES ('181', '91', 'vod.test.dnsgslb.c4hcdn.com', '2', 'imgsource', 'k22W-RXMajahZi9cryGL9sZspq_uFtYf', '0', 'tcmyaocz.bucket.efs.gsgslb.com', 'vod.test.dnsgslb.c4hcdn.com', 'I8PrfSGfLubSvh11MexrRFTLn_4FWRMb', '1', '', '', '2018-01-11 18:48:12');
INSERT INTO bucket VALUES ('183', '103', 'cdn.yusi.tv', '2', 'imgsource', '_V4JdvuIO3-B7cDcvnzCRbhwrZTJUcv0', '0', '5cv1ipcz.bucket.efs.gsgslb.com', 'cdn.yusi.tv', 'OmCgCZx0wwTklRSgS_3XKxpdRmSvO72y', '2', '', '', '2018-01-12 10:40:33');
INSERT INTO bucket VALUES ('185', '103', 'cfa.live.moguv.com', '2', 'imgsource', '_V4JdvuIO3-B7cDcvnzCRbhwrZTJUcv0', '0', '5ca2ipcz.bucket.efs.gsgslb.com', 'cfa.live.moguv.com', 'OmCgCZx0wwTklRSgS_3XKxpdRmSvO72y', '2', '', '', '2018-01-12 10:40:48');
INSERT INTO bucket VALUES ('187', '103', 'csl.live.moguv.com', '2', 'imgsource', '_V4JdvuIO3-B7cDcvnzCRbhwrZTJUcv0', '0', '5cr2ipcz.bucket.efs.gsgslb.com', 'csl.live.moguv.com', 'OmCgCZx0wwTklRSgS_3XKxpdRmSvO72y', '2', '', '', '2018-01-12 10:41:05');
INSERT INTO bucket VALUES ('189', '103', 'downgs.aginomoto.com', '2', 'imgsource', '_V4JdvuIO3-B7cDcvnzCRbhwrZTJUcv0', '0', '5ca3ipcz.bucket.efs.gsgslb.com', 'downgs.aginomoto.com', 'OmCgCZx0wwTklRSgS_3XKxpdRmSvO72y', '2', '', '', '2018-01-12 10:41:24');
INSERT INTO bucket VALUES ('191', '103', 'live.moguv.com', '2', 'imgsource', '_V4JdvuIO3-B7cDcvnzCRbhwrZTJUcv0', '0', '5cn3ipcz.bucket.efs.gsgslb.com', 'live.moguv.com', 'OmCgCZx0wwTklRSgS_3XKxpdRmSvO72y', '2', '', '', '2018-01-12 10:41:37');
INSERT INTO bucket VALUES ('193', '103', 'mediags.download.moguv.com', '2', 'imgsource', '_V4JdvuIO3-B7cDcvnzCRbhwrZTJUcv0', '0', '5cw3ipcz.bucket.efs.gsgslb.com', 'mediags.download.moguv.com', 'OmCgCZx0wwTklRSgS_3XKxpdRmSvO72y', '2', '', '', '2018-01-12 10:41:46');
INSERT INTO bucket VALUES ('195', '103', 'mediags.metis.tvmore.com.cn', '2', 'imgsource', '_V4JdvuIO3-B7cDcvnzCRbhwrZTJUcv0', '0', '5cc4ipcz.bucket.efs.gsgslb.com', 'mediags.metis.tvmore.com.cn', 'OmCgCZx0wwTklRSgS_3XKxpdRmSvO72y', '2', '', '', '2018-01-12 10:42:02');
INSERT INTO bucket VALUES ('197', '103', 'mediags.moretv.com.cn', '2', 'imgsource', '_V4JdvuIO3-B7cDcvnzCRbhwrZTJUcv0', '0', '5cj4ipcz.bucket.efs.gsgslb.com', 'mediags.moretv.com.cn', 'OmCgCZx0wwTklRSgS_3XKxpdRmSvO72y', '2', '', '', '2018-01-12 10:42:09');
INSERT INTO bucket VALUES ('201', '103', 'mediags.vr.moguv.com', '2', 'imgsource', '_V4JdvuIO3-B7cDcvnzCRbhwrZTJUcv0', '0', '5c64ipcz.bucket.efs.gsgslb.com', 'mediags.vr.moguv.com', 'OmCgCZx0wwTklRSgS_3XKxpdRmSvO72y', '2', '', '', '2018-01-12 10:42:32');
INSERT INTO bucket VALUES ('203', '103', 'p2p-gs.tvmore.com.cn', '2', 'imgsource', '_V4JdvuIO3-B7cDcvnzCRbhwrZTJUcv0', '0', '5cb5ipcz.bucket.efs.gsgslb.com', 'p2p-gs.tvmore.com.cn', 'OmCgCZx0wwTklRSgS_3XKxpdRmSvO72y', '2', '', '', '2018-01-12 10:42:37');
INSERT INTO bucket VALUES ('205', '103', 'down-office.whaley.cn', '2', 'imgsource', '_V4JdvuIO3-B7cDcvnzCRbhwrZTJUcv0', '0', '5co5ipcz.bucket.efs.gsgslb.com', 'down-office.whaley.cn', 'OmCgCZx0wwTklRSgS_3XKxpdRmSvO72y', '2', '', '', '2018-01-12 10:42:50');
INSERT INTO bucket VALUES ('207', '103', 'vadgs.tvmore.com.cn', '2', 'imgsource', '_V4JdvuIO3-B7cDcvnzCRbhwrZTJUcv0', '0', '5cx5ipcz.bucket.efs.gsgslb.com', 'vadgs.tvmore.com.cn', 'OmCgCZx0wwTklRSgS_3XKxpdRmSvO72y', '2', '', '', '2018-01-12 10:42:59');

-- ----------------------------
-- Table structure for `region`
-- ----------------------------
DROP TABLE IF EXISTS `region`;
CREATE TABLE `region` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `region_name` text NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=MyISAM AUTO_INCREMENT=4 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of region
-- ----------------------------
INSERT INTO region VALUES ('2', '华东');
INSERT INTO region VALUES ('1', '华北');
