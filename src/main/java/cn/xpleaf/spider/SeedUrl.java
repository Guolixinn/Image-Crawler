package cn.xpleaf.spider;

import cn.xpleaf.spider.constants.SpiderConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import static cn.xpleaf.spider.utils.SpiderUtil.getTopDomain;

public class SeedUrl {
//    private static final String TABLE_NAME = "image_urls";
//    private static final String COLUMN_FAMILY = "url_info";
//    private static final String COLUMN_QUALIFIER = "image_url";

    public static void main(String[] args) {
        // 种子 URL 列表
        List<String> seedUrls = Arrays.asList(
                "https://www.hippopx.com/en/query?q=fruit"
//                "https://www.example2.com",
//                "https://www.example3.com"
        );

        // URL 队列
        Queue<String> urlQueue = new LinkedList<>(seedUrls);
        // 从队列里取出一个 URL，使用 Jsoup 解析该网页，提取其中的所有链接，再把这些链接添加到 URL 队列中
        if (!urlQueue.isEmpty()) {
            String currentUrl = urlQueue.poll();
            try {
                Document doc = Jsoup.connect(currentUrl).get();
                Elements links = doc.select("a[href]");
                for (Element link : links) {
                    String newUrl = link.absUrl("href");
                    if (!newUrl.isEmpty()) {
                        urlQueue.add(newUrl);
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        // 连接到 HBase
        Configuration config = HBaseConfiguration.create();
        try {
            SpiderConstants spiderConstants = new SpiderConstants() {};
            spiderConstants.initHBaseTable();

            try (Connection connection = ConnectionFactory.createConnection(config);
                 Table table = connection.getTable(TableName.valueOf(SpiderConstants.TABLE_NAME))) {
                while (!urlQueue.isEmpty()) {
                    String url = urlQueue.poll();
                    // 这里简单使用 URL 的哈希码作为行键
                    Put put = new Put(Bytes.toBytes(url.hashCode()));
                    put.addColumn(Bytes.toBytes(SpiderConstants.COLUMN_FAMILY_URL_SEED), Bytes.toBytes(SpiderConstants.COLUMN_QUALIFIER_URL), Bytes.toBytes(url));
                    put.addColumn(Bytes.toBytes(SpiderConstants.COLUMN_FAMILY_DOMAIN), Bytes.toBytes(SpiderConstants.COLUMN_QUALIFIER_DOMAIN_NAME ), Bytes.toBytes(getTopDomain(url)));
                    System.out.println(getTopDomain(url));
                    put.addColumn(Bytes.toBytes(SpiderConstants.COLUMN_FAMILY_PRIORITY), Bytes.toBytes(SpiderConstants.COLUMN_QUALIFIER_PRIORITY_VALUE ), Bytes.toBytes(false));
                    table.put(put);
                    // 模拟延迟
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 连接到 HBase
//        Configuration config = HBaseConfiguration.create();package cn.xpleaf.spider;
//
//import cn.xpleaf.spider.constants.SpiderConstants;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.hbase.HBaseConfiguration;
//import org.apache.hadoop.hbase.TableName;
//import org.apache.hadoop.hbase.client.*;
//import org.apache.hadoop.hbase.util.Bytes;
//import org.jsoup.Jsoup;
//import org.jsoup.nodes.Document;
//import org.jsoup.nodes.Element;
//import org.jsoup.select.Elements;
//import java.io.IOException;
//import java.util.Arrays;
//import java.util.LinkedList;
//import java.util.List;
//import java.util.Queue;
//
//public class SeedUrl {
////    private static final String TABLE_NAME = "image_urls";
////    private static final String COLUMN_FAMILY = "url_info";
////    private static final String COLUMN_QUALIFIER = "image_url";
//
//    public static void main(String[] args) {
//        // 种子 URL 列表
//        List<String> seedUrls = Arrays.asList(
//                "https://www.hippopx.com/en/query?q=fruit"
////                "https://www.example2.com",
////                "https://www.example3.com"
//        );
//
//        // URL 队列
//        Queue<String> urlQueue = new LinkedList<>(seedUrls);
//        // 从队列里取出一个 URL，使用 Jsoup 解析该网页，提取其中的所有链接，再把这些链接添加到 URL 队列中
//        if (!urlQueue.isEmpty()) {
//            String currentUrl = urlQueue.poll();
//            try {
//                Document doc = Jsoup.connect(currentUrl).get();
//                Elements links = doc.select("a[href]");
//                for (Element link : links) {
//                    String newUrl = link.absUrl("href");
//                    if (!newUrl.isEmpty()) {
//                        urlQueue.add(newUrl);
//                    }
//                }
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
//
//        // 连接到 HBase
//        Configuration config = HBaseConfiguration.create();
//        try {
//            SpiderConstants spiderConstants = new SpiderConstants() {};
//            spiderConstants.initHBaseTable();
//
//            try (Connection connection = ConnectionFactory.createConnection(config);
//                 Table table = connection.getTable(TableName.valueOf(SpiderConstants.TABLE_NAME))) {
//                while (!urlQueue.isEmpty()) {
//                    String url = urlQueue.poll();
//                    // 这里简单使用 URL 的哈希码作为行键
//                    Put put = new Put(Bytes.toBytes(url.hashCode()));
//                    put.addColumn(Bytes.toBytes(SpiderConstants.COLUMN_FAMILY_URL_SEED), Bytes.toBytes(SpiderConstants.COLUMN_QUALIFIER_URL), Bytes.toBytes(url));
//                    table.put(put);
//                    // 模拟延迟
//                    try {
//                        Thread.sleep(1000);
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
//                }
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//        // 连接到 HBase
////        Configuration config = HBaseConfiguration.create();
////        try (Connection connection = ConnectionFactory.createConnection(config);
////             Admin admin = connection.getAdmin()) {
////
////            TableName tableName = TableName.valueOf(SpiderConstants.TABLE_NAME);
////            if (!admin.tableExists(tableName)) {
////                // 创建表描述符
////                HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
////                // 创建列族描述符
////                HColumnDescriptor columnFamilyDescriptor = new HColumnDescriptor(SpiderConstants.COLUMN_FAMILY_URL_SEED);
////                tableDescriptor.addFamily(columnFamilyDescriptor);
////                // 创建表
////                admin.createTable(tableDescriptor);
////            }
////
////            try (Table table = connection.getTable(tableName)) {
////                while (!urlQueue.isEmpty()) {
////                    String url = urlQueue.poll();
////                    // 这里简单使用 URL 的哈希码作为行键
////                    Put put = new Put(Bytes.toBytes(url.hashCode()));
////                    put.addColumn(Bytes.toBytes(SpiderConstants.COLUMN_FAMILY_URL_SEED), Bytes.toBytes(SpiderConstants.COLUMN_QUALIFIER_URL), Bytes.toBytes(url));
////                    table.put(put);
////                    // 模拟延迟
////                    try {
////                        Thread.sleep(1000);
////                    } catch (InterruptedException e) {
////                        e.printStackTrace();
////                    }
////                }
////            }
////        } catch (IOException e) {
////            e.printStackTrace();
////        }
//    }
//}
//
//// 以下是源码：
//
////import cn.xpleaf.spider.constants.SpiderConstants;
////import cn.xpleaf.spider.utils.JedisUtil;
////import cn.xpleaf.spider.utils.SpiderUtil;
////import redis.clients.jedis.Jedis;
//
///**
// * @author hong
// * @date 2022年08月20日 17:00
// */
////public class SeedUrl {
////
////    public static void main(String[] args) {
////        Jedis jedis = JedisUtil.getJedis();
////        for (int i = 3; i < 50; i+=2) {
////            String url = "https://list.jd.com/list.html?cat=9987,653,655&page="+i;
////            String domain = SpiderUtil.getTopDomain(url);   // 获取url对应的顶级域名，如jd.com
////            String key = domain + SpiderConstants.SPIDER_DOMAIN_HIGHER_SUFFIX;            // 拼接url队列的key，如jd.com.higher
////            jedis.lpush(key, url);
////            SpiderUtil.sleep(1000);
////        }
////        JedisUtil.returnJedis(jedis);
////    }
////}
//        try (Connection connection = ConnectionFactory.createConnection(config);
//             Admin admin = connection.getAdmin()) {
//
//            TableName tableName = TableName.valueOf(SpiderConstants.TABLE_NAME);
//            if (!admin.tableExists(tableName)) {
//                // 创建表描述符
//                HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
//                // 创建列族描述符
//                HColumnDescriptor columnFamilyDescriptor = new HColumnDescriptor(SpiderConstants.COLUMN_FAMILY_URL_SEED);
//                tableDescriptor.addFamily(columnFamilyDescriptor);
//                // 创建表
//                admin.createTable(tableDescriptor);
//            }
//
//            try (Table table = connection.getTable(tableName)) {
//                while (!urlQueue.isEmpty()) {
//                    String url = urlQueue.poll();
//                    // 这里简单使用 URL 的哈希码作为行键
//                    Put put = new Put(Bytes.toBytes(url.hashCode()));
//                    put.addColumn(Bytes.toBytes(SpiderConstants.COLUMN_FAMILY_URL_SEED), Bytes.toBytes(SpiderConstants.COLUMN_QUALIFIER_URL), Bytes.toBytes(url));
//                    table.put(put);
//                    // 模拟延迟
//                    try {
//                        Thread.sleep(1000);
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
//                }
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
    }
}

// 以下是源码：

//import cn.xpleaf.spider.constants.SpiderConstants;
//import cn.xpleaf.spider.utils.JedisUtil;
//import cn.xpleaf.spider.utils.SpiderUtil;
//import redis.clients.jedis.Jedis;

/**
 * @author hong
 * @date 2022年08月20日 17:00
 */
//public class SeedUrl {
//
//    public static void main(String[] args) {
//        Jedis jedis = JedisUtil.getJedis();
//        for (int i = 3; i < 50; i+=2) {
//            String url = "https://list.jd.com/list.html?cat=9987,653,655&page="+i;
//            String domain = SpiderUtil.getTopDomain(url);   // 获取url对应的顶级域名，如jd.com
//            String key = domain + SpiderConstants.SPIDER_DOMAIN_HIGHER_SUFFIX;            // 拼接url队列的key，如jd.com.higher
//            jedis.lpush(key, url);
//            SpiderUtil.sleep(1000);
//        }
//        JedisUtil.returnJedis(jedis);
//    }
//}
