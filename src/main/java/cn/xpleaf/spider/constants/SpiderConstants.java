package cn.xpleaf.spider.constants;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// 定义一个接口来管理爬虫相关的 URL 信息
public interface SpiderConstants {

    // 常量定义，用于 HBase 表结构
    String TABLE_NAME = "spider_url_info";
    String COLUMN_FAMILY_URL_SEED = "url_seed";
    String COLUMN_QUALIFIER_URL = "url";
    String COLUMN_FAMILY_DOMAIN = "domain";
    String COLUMN_QUALIFIER_DOMAIN_NAME = "domain_name";
    String COLUMN_FAMILY_PRIORITY = "priority";
    String COLUMN_QUALIFIER_PRIORITY_VALUE = "priority_value";
//    String COLUMN_FAMILY_LOCK = "lock";
//    String COLUMN_QUALIFIER_LOCK_VALUE = "lock_value";


    // 初始化 HBase 表
    default void initHBaseTable() throws IOException {
        Configuration config = HBaseConfiguration.create();
        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {

            TableName tableName = TableName.valueOf(TABLE_NAME);
            if (!admin.tableExists(tableName)) {
                // 创建表描述符
                HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);

                // 创建 URL 种子列族描述符
                HColumnDescriptor urlSeedColumnFamilyDescriptor = new HColumnDescriptor(COLUMN_FAMILY_URL_SEED);
                tableDescriptor.addFamily(urlSeedColumnFamilyDescriptor);

                // 创建网站域名列族描述符
                HColumnDescriptor domainColumnFamilyDescriptor = new HColumnDescriptor(COLUMN_FAMILY_DOMAIN);
                tableDescriptor.addFamily(domainColumnFamilyDescriptor);

                // 创建 URL 优先级列族描述符
                HColumnDescriptor priorityColumnFamilyDescriptor = new HColumnDescriptor(COLUMN_FAMILY_PRIORITY);
                tableDescriptor.addFamily(priorityColumnFamilyDescriptor);

                // 创建表
                admin.createTable(tableDescriptor);
            }
        }
    }

    // 添加 URL 种子到 HBase，并设置是否已解析
    default void addUrlSeed(String url, boolean isParsed) throws IOException {
        Configuration config = HBaseConfiguration.create();
        try (Connection connection = ConnectionFactory.createConnection(config);
             Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {
            Put put = new Put(Bytes.toBytes(url.hashCode()));
            put.addColumn(Bytes.toBytes(COLUMN_FAMILY_URL_SEED), Bytes.toBytes(COLUMN_QUALIFIER_URL), Bytes.toBytes(url));
            put.addColumn(Bytes.toBytes(COLUMN_FAMILY_PRIORITY), Bytes.toBytes(COLUMN_QUALIFIER_PRIORITY_VALUE), Bytes.toBytes(isParsed));
            String domain = getDomainFromUrl(url);
            put.addColumn(Bytes.toBytes(COLUMN_FAMILY_DOMAIN), Bytes.toBytes(COLUMN_QUALIFIER_DOMAIN_NAME), Bytes.toBytes(domain));
            table.put(put);
        }
    }

    // 从 URL 中提取域名
    default String getDomainFromUrl(String url) {
        // 简单示例，实际可能需要更复杂的处理
        int startIndex = url.indexOf("//") + 2;
        int endIndex = url.indexOf("/", startIndex);
        if (endIndex == -1) {
            endIndex = url.length();
        }
        return url.substring(startIndex, endIndex);
    }

    // 获取所有 URL 种子
    default List<String> getAllUrlSeeds() throws IOException {
        // 实现获取所有 URL 种子的逻辑
        List<String> urlSeeds = new ArrayList<>();
        Configuration config = HBaseConfiguration.create();
        try (Connection connection = ConnectionFactory.createConnection(config);
             Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {
            // 创建扫描器
            Scan scan = new Scan();
            // 指定要扫描的列族和列限定符
            scan.addColumn(Bytes.toBytes(COLUMN_FAMILY_URL_SEED), Bytes.toBytes(COLUMN_QUALIFIER_URL));
            try (ResultScanner scanner = table.getScanner(scan)) {
                for (Result result : scanner) {
                    byte[] value = result.getValue(Bytes.toBytes(COLUMN_FAMILY_URL_SEED), Bytes.toBytes(COLUMN_QUALIFIER_URL));
                    if (value != null) {
                        urlSeeds.add(Bytes.toString(value));
                    }
                }
            }
        }
        return urlSeeds;
    }

    // 获取指定域名的 URL 种子
    default List<String> getUrlSeedsByDomain(String domain) throws IOException {
        List<String> urlSeeds = new ArrayList<>();
        Configuration config = HBaseConfiguration.create();
        try (Connection connection = ConnectionFactory.createConnection(config);
             Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {
            // 创建扫描器
            Scan scan = new Scan();
            // 指定要扫描的列族和列限定符
            scan.addColumn(Bytes.toBytes(COLUMN_FAMILY_DOMAIN), Bytes.toBytes(COLUMN_QUALIFIER_DOMAIN_NAME));
            scan.addColumn(Bytes.toBytes(COLUMN_FAMILY_URL_SEED), Bytes.toBytes(COLUMN_QUALIFIER_URL));
            try (ResultScanner scanner = table.getScanner(scan)) {
                for (Result result : scanner) {
                    byte[] domainValue = result.getValue(Bytes.toBytes(COLUMN_FAMILY_DOMAIN), Bytes.toBytes(COLUMN_QUALIFIER_DOMAIN_NAME));
                    if (domainValue != null && Bytes.toString(domainValue).equals(domain)) {
                        byte[] urlValue = result.getValue(Bytes.toBytes(COLUMN_FAMILY_URL_SEED), Bytes.toBytes(COLUMN_QUALIFIER_URL));
                        if (urlValue != null) {
                            urlSeeds.add(Bytes.toString(urlValue));
                        }
                    }
                }
            }
        }
        return urlSeeds;
    }

    // 获取 URL 及其是否已解析的映射
    default Map<String, Boolean> getUrlParsedStatus() throws IOException {
        Map<String, Boolean> urlParsedStatus = new HashMap<>();
        Configuration config = HBaseConfiguration.create();
        try (Connection connection = ConnectionFactory.createConnection(config);
             Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {
            // 创建扫描器
            Scan scan = new Scan();
            // 指定要扫描的列族和列限定符
            scan.addColumn(Bytes.toBytes(COLUMN_FAMILY_URL_SEED), Bytes.toBytes(COLUMN_QUALIFIER_URL));
            scan.addColumn(Bytes.toBytes(COLUMN_FAMILY_PRIORITY), Bytes.toBytes(COLUMN_QUALIFIER_PRIORITY_VALUE));
            try (ResultScanner scanner = table.getScanner(scan)) {
                for (Result result : scanner) {
                    byte[] urlValue = result.getValue(Bytes.toBytes(COLUMN_FAMILY_URL_SEED), Bytes.toBytes(COLUMN_QUALIFIER_URL));
                    byte[] priorityValue = result.getValue(Bytes.toBytes(COLUMN_FAMILY_PRIORITY), Bytes.toBytes(COLUMN_QUALIFIER_PRIORITY_VALUE));
                    if (urlValue != null && priorityValue != null) {
                        String url = Bytes.toString(urlValue);
                        boolean isParsed = Bytes.toBoolean(priorityValue);
                        urlParsedStatus.put(url, isParsed);
                    }
                }
            }
        }
        return urlParsedStatus;
    }
}







//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.hbase.HBaseConfiguration;
//import org.apache.hadoop.hbase.HColumnDescriptor;
//import org.apache.hadoop.hbase.HTableDescriptor;
//import org.apache.hadoop.hbase.TableName;
//import org.apache.hadoop.hbase.client.*;
//import org.apache.hadoop.hbase.util.Bytes;
//
//import java.io.IOException;
//
//public class SpiderConstants {
//    // 修改常量定义
//    public static final String TABLE_NAME = "image_urls";
//    public static final String COLUMN_FAMILY = "url_info";
//    public static final String COLUMN_QUALIFIER = "image_url";
////    private static final String COLUMN_FAMILY_METADATA = "metadata";
////    private static final String COLUMN_QUALIFIER_WIDTH = "width";
////    private static final String COLUMN_QUALIFIER_HEIGHT = "height";
////    private static final String COLUMN_QUALIFIER_FORMAT = "format";
//
//    public static void main(String[] args) throws IOException {
//        Configuration config = HBaseConfiguration.create();
//        try (Connection connection = ConnectionFactory.createConnection(config);
//             Admin admin = connection.getAdmin()) {
//
//            TableName tableName = TableName.valueOf(TABLE_NAME);
//            if (!admin.tableExists(tableName)) {
//                // 使用 HBase 1.1.5 兼容的 API 创建表和列族
//                HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
//
//                // 创建 URL 列族描述符
//                HColumnDescriptor urlColumnFamilyDescriptor = new HColumnDescriptor(COLUMN_FAMILY);
//                tableDescriptor.addFamily(urlColumnFamilyDescriptor);
//
//                // 创建元数据列族描述符
////                HColumnDescriptor metadataColumnFamilyDescriptor = new HColumnDescriptor(COLUMN_FAMILY_METADATA);
////                tableDescriptor.addFamily(metadataColumnFamilyDescriptor);
//
//                // 创建表
//                admin.createTable(tableDescriptor);
//            }
//
//            try (Table table = connection.getTable(tableName)) {
//                String imageUrl = "https://example.com/image.jpg";
////                int width = 800;
////                int height = 600;
////                String format = "JPEG";
//
//                Put put = new Put(Bytes.toBytes(imageUrl.hashCode()));
////                put.addColumn(Bytes.toBytes(COLUMN_FAMILY_URL), Bytes.toBytes(COLUMN_QUALIFIER_IMAGE_URL), Bytes.toBytes(imageUrl));
////                put.addColumn(Bytes.toBytes(COLUMN_FAMILY_METADATA), Bytes.toBytes(COLUMN_QUALIFIER_WIDTH), Bytes.toBytes(width));
////                put.addColumn(Bytes.toBytes(COLUMN_FAMILY_METADATA), Bytes.toBytes(COLUMN_QUALIFIER_HEIGHT), Bytes.toBytes(height));
////                put.addColumn(Bytes.toBytes(COLUMN_FAMILY_METADATA), Bytes.toBytes(COLUMN_QUALIFIER_FORMAT), Bytes.toBytes(format));
//                put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(COLUMN_QUALIFIER), Bytes.toBytes(imageUrl));
//                table.put(put);
//            }
//        }
//    }
//}
// 解释：
//数据插入的目标位置
//数据会被插入到 HBase 的指定表中，具体信息如下：
//表名：由 TABLE_NAME 常量指定，在之前的 SpiderConstants.java 文件中，TABLE_NAME 被定义为 "image_urls"，所以数据会插入到名为 image_urls 的 HBase 表中。
//列族：由 COLUMN_FAMILY 常量指定，这里是 "url_info"，表示数据会存储在该表的 url_info 列族下。
//列限定符：由 COLUMN_QUALIFIER 常量指定，为 "image_url"，意味着数据会存储在 url_info 列族下名为 image_url 的列中。
//行键：使用 imageUrl.hashCode() 作为行键。hashCode() 方法会返回该 URL 的哈希码，然后通过 Bytes.toBytes() 方法将其转换为字节数组。行键用于唯一标识 HBase 表中的每一行数据，这样可以确保不同的图片 URL 对应不同的行。


// 以下代码是针对redis设计的
//目前代码中的常量设计是基于 Redis 的数据结构和使用方式的，主要适配 Redis。
// 例如，使用 set 数据结构进行去重，使用 list 数据结构进行 URL 的存储和弹出操作，这些都是 Redis 特有的数据结构和操作。
//package cn.xpleaf.spider.constants;
//
//public interface SpiderConstants {
//
//    // url种子仓库的key，redis中存储数据类型为set，防止重复添加种子url
//    public String SPIDER_SEED_URLS_KEY = "spider.seed.urls";
//
//    // 获取的网站顶级域名集合，redis中存储数据类型为set
//    public String SPIDER_WEBSITE_DOMAINS_KEY = "spider.website.domains";
//
//    // 获取网站高优先级url的后缀，redis中存储数据类型为list
//    String SPIDER_DOMAIN_HIGHER_SUFFIX = ".higher";
//
//    // 获取网站低优先级url的后缀，redis中存储数据类型为list
//    String SPIDER_DOMAIN_LOWER_SUFFIX = ".lower";
//}
