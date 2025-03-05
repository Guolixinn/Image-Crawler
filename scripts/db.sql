-- 创建数据库，如果不存在则创建
CREATE DATABASE IF NOT EXISTS ispider;

-- -- 切换到 ispider 数据库
-- USE ispider;

-- 如果 image 表存在则删除
DROP TABLE IF EXISTS `image`;
-- 创建 image 表
CREATE TABLE `image` (
    -- id 字段改为 varchar(36) 类型，用于存储字符串形式的唯一标识，例如 UUID 格式
                         `id` varchar(36) NOT NULL COMMENT '图片唯一标识',
    -- 图片地址，最大长度 500
                         `img_url` varchar(500) DEFAULT NULL COMMENT '图片地址',
    -- 图片数据，使用 LONGBLOB 类型存储二进制数据
                         `image_data` LONGBLOB DEFAULT NULL COMMENT '图片数据',
    -- 图片尺寸，使用 varchar(20) 类型存储，例如 '1234*5678'
                         `image_size` varchar(20) DEFAULT NULL COMMENT '图片尺寸，如 1234*5678',
    -- 图片大小，最大长度 100
                         `image_memory` varchar(100) DEFAULT NULL COMMENT '图片内存',
    -- 图片标签，多个标签用逗号分隔，最大长度 500
                         `tags` varchar(500) DEFAULT NULL COMMENT '图片标签，多个标签用逗号分隔',
    -- 指定 id 列为主键
                         PRIMARY KEY (`id`)'指定 id 列为主键'
--                          PRIMARY KEY (id) COMMENT '指定 id 列为主键'
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


-- CREATE DATABASE IF NOT EXISTS ispider;
-- DROP TABLE IF EXISTS `phone`;
-- CREATE TABLE `phone` (
--   `id` varchar(30) CHARACTER SET armscii8 NOT NULL COMMENT '商品id',
--   `source` varchar(30) NOT NULL COMMENT '商品来源，如jd suning gome等',
--   `brand` varchar(30) DEFAULT NULL COMMENT '手机品牌',
--   `title` varchar(255) DEFAULT NULL COMMENT '商品页面的手机标题',
--   `price` float(10,2) DEFAULT NULL COMMENT '手机价格',
--   `comment_count` varchar(30) DEFAULT NULL COMMENT '手机评论',
--   `good_rate` float(10,3) DEFAULT NULL COMMENT '好评率',
--   `url` varchar(500) DEFAULT NULL COMMENT '手机详细信息地址',
--   `img_url` varchar(500) DEFAULT NULL COMMENT '图片地址',
--   `main` varchar(500) DEFAULT NULL COMMENT '主体，产品名称',
--   `battery` varchar(500) DEFAULT NULL COMMENT '电池信息',
--   `interface` varchar(500) DEFAULT NULL COMMENT '数据接口',
--   `network` varchar(500) DEFAULT NULL COMMENT '网络支持',
--   `operating_system` varchar(500) DEFAULT NULL COMMENT '操作系统',
--   `basic_info` varchar(500) DEFAULT NULL COMMENT '基本信息',
--   `camera` varchar(500) DEFAULT NULL COMMENT '摄像头',
--   `screen` varchar(500) DEFAULT NULL COMMENT '屏幕',
--   PRIMARY KEY (`id`,`source`)
-- ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
--
-- SELECT * FROM phone WHERE title LIKE '%RedMi%';