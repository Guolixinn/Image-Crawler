package cn.xpleaf.spider.constants;

/**
 * 专门用于存放HBase配置的常量类
 */
public interface HBaseConstants {

    // HBase的ZooKeeper集群地址
    String HBASE_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
    // HBase的ZooKeeper客户端端口
    String HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT = "hbase.zookeeper.property.clientPort";

    // HBase表名，匹配SpiderConstants中的表名
    String HBASE_TABLE_NAME = SpiderConstants.TABLE_NAME;
    // URL种子列族名，匹配SpiderConstants中的列族名
    String HBASE_COLUMN_FAMILY_URL_SEED = SpiderConstants.COLUMN_FAMILY_URL_SEED;
    // URL列限定符名，匹配SpiderConstants中的列限定符名
    String HBASE_COLUMN_QUALIFIER_URL = SpiderConstants.COLUMN_QUALIFIER_URL;
    // 域名列族名，匹配SpiderConstants中的列族名
    String HBASE_COLUMN_FAMILY_DOMAIN = SpiderConstants.COLUMN_FAMILY_DOMAIN;
    // 域名列限定符名，匹配SpiderConstants中的列限定符名
    String HBASE_COLUMN_QUALIFIER_DOMAIN_NAME = SpiderConstants.COLUMN_QUALIFIER_DOMAIN_NAME;
    // 优先级列族名，匹配SpiderConstants中的列族名
    String HBASE_COLUMN_FAMILY_PRIORITY = SpiderConstants.COLUMN_FAMILY_PRIORITY;
    // 优先级值列限定符名，匹配SpiderConstants中的列限定符名
    String HBASE_COLUMN_QUALIFIER_PRIORITY_VALUE = SpiderConstants.COLUMN_QUALIFIER_PRIORITY_VALUE;

    // HBase客户端最大连接数
    String HBASE_CLIENT_MAX_TOTAL = "hbase.client.max.total";
    // HBase客户端最大空闲连接数
    String HBASE_CLIENT_MAX_IDLE = "hbase.client.max.idle";
    // HBase客户端最小空闲连接数
    String HBASE_CLIENT_MIN_IDLE = "hbase.client.min.idle";

    // HBase客户端最大等待连接时间 ms值
    String HBASE_CLIENT_MAX_WAIT_MILLIS = "hbase.client.max.wait.millis";
}



//package cn.xpleaf.spider.constants;
//
///**
// * 专门用于存放Jedis的常量类
// */
//public interface JedisConstants {
//
//    // 表示jedis的服务器主机名
//    String JEDIS_HOST = "jedis.host";
//    // 表示jedis的服务的端口
//    String JEDIS_PORT = "jedis.port";
//    // 表示jedis的服务密码
//    String JEDIS_PASSWORD = "jedis.password";
//
//    // jedis连接池中最大的连接个数
//    String JEDIS_MAX_TOTAL = "jedis.max.total";
//    // jedis连接池中最大的空闲连接个数
//    String JEDIS_MAX_IDLE = "jedis.max.idle";
//    // jedis连接池中最小的空闲连接个数
//    String JEDIS_MIN_IDLE = "jedis.min.idle";
//
//    // jedis连接池最大的等待连接时间 ms值
//    String JEDIS_MAX_WAIT_MILLIS = "jedis.max.wait.millis";
//
//}
