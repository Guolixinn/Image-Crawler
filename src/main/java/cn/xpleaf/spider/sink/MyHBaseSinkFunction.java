package cn.xpleaf.spider.sink;

import cn.xpleaf.spider.constants.SpiderConstants;
import cn.xpleaf.spider.utils.SpiderUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction.Context;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import cn.xpleaf.spider.core.pojo.UrlList;
import cn.xpleaf.spider.utils.HBaseUtil;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import static cn.xpleaf.spider.utils.SpiderUtil.getTopDomain;

//// 定义 UrlList 类，包含 URL 及其优先级信息
//class UrlList {
//    private Map<String, Integer> urlPriorityMap;
//
//    public UrlList(Map<String, Integer> urlPriorityMap) {
//        this.urlPriorityMap = urlPriorityMap;
//    }
//
//    public Map<String, Integer> getUrlPriorityMap() {
//        return urlPriorityMap;
//    }
//}

/**
 * MyHBaseSinkFunction 类继承自 RichSinkFunction<UrlList>，用于将 UrlList 类型的数据写入 HBase。
 */
public class MyHBaseSinkFunction extends RichSinkFunction<cn.xpleaf.spider.core.pojo.UrlList> {
    private static final Logger LOG = LoggerFactory.getLogger(MyHBaseSinkFunction.class);

    private Connection connection;
    private Table table;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        try {
            // 创建 HBase 配置
            org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
            // 创建 HBase 连接
            connection = ConnectionFactory.createConnection(config);
            boolean tableExists = HBaseUtil.isTableExists();
            if (tableExists){
                System.out.println("HBase 表存在");
            } else {
                System.out.println("HBase 表不存在");
            }
            // 获取表对象
            table = connection.getTable(TableName.valueOf(SpiderConstants.TABLE_NAME));


        } catch (IOException e) {
            LOG.error("Failed to open HBase connection or get table.", e);
            throw new RuntimeException("Failed to open HBase connection or get table.", e);
        }
    }

    @Override
    public void invoke(cn.xpleaf.spider.core.pojo.UrlList value, Context context) throws Exception {
        List<String> urlList_write = value.getList();
        if(!urlList_write.isEmpty()){
            for(String url:urlList_write){
                String domain = SpiderUtil.getTopDomain(url);   // 获取url对应的顶级域名，如jd.com
                // 这里简单使用 URL 的哈希码作为行键
                Put put = new Put(Bytes.toBytes(url.hashCode()));
                put.addColumn(Bytes.toBytes(SpiderConstants.COLUMN_FAMILY_URL_SEED), Bytes.toBytes(SpiderConstants.COLUMN_QUALIFIER_URL), Bytes.toBytes(url));
                put.addColumn(Bytes.toBytes(SpiderConstants.COLUMN_FAMILY_DOMAIN), Bytes.toBytes(SpiderConstants.COLUMN_QUALIFIER_DOMAIN_NAME ), Bytes.toBytes(domain));
                System.out.println(getTopDomain(url));
                put.addColumn(Bytes.toBytes(SpiderConstants.COLUMN_FAMILY_PRIORITY), Bytes.toBytes(SpiderConstants.COLUMN_QUALIFIER_PRIORITY_VALUE ), Bytes.toBytes(false));
                table.put(put);
            }
            Scan scan = new Scan();
            try (ResultScanner scanner = table.getScanner(scan)) {
                // 遍历扫描结果
                for (Result result : scanner) {
                    // 输出行键
                    byte[] rowKey = result.getRow();
                    System.out.println("Row Key: " + Bytes.toString(rowKey));

                    // 输出每个单元格的信息
                    Cell[] cells = result.rawCells();
                    for (Cell cell : cells) {
                        // 获取列族
                        byte[] family = CellUtil.cloneFamily(cell);
                        // 获取列限定符
                        byte[] qualifier = CellUtil.cloneQualifier(cell);
                        // 获取值
                        byte[] value1 = CellUtil.cloneValue(cell);

                        System.out.println("  Column Family: " + Bytes.toString(family) +
                                ", Column Qualifier: " + Bytes.toString(qualifier) +
                                ", Value: " + Bytes.toString(value1));

                    }
                }
//                System.out.println('1');
            }

        }

//        Map<String, Integer> urlPriorityMap = value;
//        for (Map.Entry<String, Integer> entry : urlPriorityMap.entrySet()) {
//            String url = entry.getKey();
//            int priority = entry.getValue();
//            try {
//                // 这里简单使用 URL 的哈希码作为行键
//                Put put = new Put(Bytes.toBytes(url.hashCode()));
//                put.addColumn(Bytes.toBytes(SpiderConstants.COLUMN_FAMILY_URL_SEED), Bytes.toBytes(SpiderConstants.COLUMN_QUALIFIER_URL), Bytes.toBytes(url));
//                // 写入 URL 优先级信息
//                put.addColumn(Bytes.toBytes(SpiderConstants.COLUMN_FAMILY_PRIORITY), Bytes.toBytes(SpiderConstants.COLUMN_QUALIFIER_PRIORITY_VALUE), Bytes.toBytes(priority));
//                // 将数据写入 HBase
//                table.put(put);
//            } catch (IOException e) {
//                LOG.error("Failed to write data to HBase for URL: " + url, e);
//            }
//        }
    }

    @Override
    public void close() throws Exception {
        try {
            if (table != null) {
                table.close();
            }
        } catch (IOException e) {
            LOG.error("Failed to close HBase table.", e);
        }
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (IOException e) {
            LOG.error("Failed to close HBase connection.", e);
        }
    }
}



//package cn.xpleaf.spider.sink;
//
//import cn.xpleaf.spider.constants.SpiderConstants;
//import cn.xpleaf.spider.core.pojo.UrlList;
//import cn.xpleaf.spider.utils.JedisUtil;
//import cn.xpleaf.spider.utils.SpiderUtil;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
//import redis.clients.jedis.Jedis;
//
//import java.util.List;
//
///**
// * @author hong
// * @date 2022年08月20日 10:27
// */
//public class MyRedisSinkFunction extends RichSinkFunction<UrlList> {
//
//    Jedis jedis = null;
//
//    public void open(Configuration parameters) throws Exception {
//        jedis = JedisUtil.getJedis();
//    }
//    public void invoke(UrlList value, Context context) {
//        List<String> high = value.getHighList();
//        List<String> low = value.getLowList();
//        if(!high.isEmpty()){
//            for(String url:high){
//                String domain = SpiderUtil.getTopDomain(url);   // 获取url对应的顶级域名，如jd.com
//                String key = domain + SpiderConstants.SPIDER_DOMAIN_HIGHER_SUFFIX;            // 拼接url队列的key，如jd.com.higher
//                jedis.lpush(key, url);
//            }
//
//        }
//        if(!low.isEmpty()){
//            for(String url:low){
//                String domain = SpiderUtil.getTopDomain(url);   // 获取url对应的顶级域名，如jd.com
//                String key = domain + SpiderConstants.SPIDER_DOMAIN_LOWER_SUFFIX;            // 拼接url队列的key，如jd.com.higher
//                jedis.lpush(key, url);
//            }
//
//        }
//
//    }
//    public void close() throws Exception {
//        super.close();
//        if(jedis != null){
//            JedisUtil.returnJedis(jedis);
//        }
//    }
//}
