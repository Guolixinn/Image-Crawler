package cn.xpleaf.spider.utils;

import cn.xpleaf.spider.constants.HBaseConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class HBaseUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseUtil.class);
    private static Configuration conf = null;
    private static Connection connection = null;

    static {
        conf = HBaseConfiguration.create();
        // 可以根据 HBaseConstants 中的配置进一步设置 Configuration
        // 例如：conf.set(HBaseConstants.HBASE_ZOOKEEPER_QUORUM, "your_zookeeper_quorum");
        // conf.set(HBaseConstants.HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT, "your_client_port");
        try {
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            LOGGER.error("创建 HBase 连接时出错", e);
        }
    }

    /**
     * 初始化表
     */
    public static void initTable() {
        try (Admin admin = connection.getAdmin()) {
            TableName tableName = TableName.valueOf(HBaseConstants.HBASE_TABLE_NAME);
            if (!admin.tableExists(tableName)) {
                // 创建表描述符
                HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);

                // 创建 URL种子列族描述符
                HColumnDescriptor urlSeedColumnFamilyDescriptor = new HColumnDescriptor(HBaseConstants.HBASE_COLUMN_FAMILY_URL_SEED);
                tableDescriptor.addFamily(urlSeedColumnFamilyDescriptor);

                // 创建网站域名列族描述符
                HColumnDescriptor domainColumnFamilyDescriptor = new HColumnDescriptor(HBaseConstants.HBASE_COLUMN_FAMILY_DOMAIN);
                tableDescriptor.addFamily(domainColumnFamilyDescriptor);

                // 创建 URL 优先级列族描述符
                HColumnDescriptor priorityColumnFamilyDescriptor = new HColumnDescriptor(HBaseConstants.HBASE_COLUMN_FAMILY_PRIORITY);
                tableDescriptor.addFamily(priorityColumnFamilyDescriptor);

                // 创建表
                admin.createTable(tableDescriptor);
            }
        } catch (IOException e) {
            LOGGER.error("初始化表时出错", e);
        }
    }

    /**
     * 从 HBase 中获取 url
     * @return
     */
    public static String getUrl() {
        try (Table table = connection.getTable(TableName.valueOf(HBaseConstants.HBASE_TABLE_NAME))) {
            Scan scan = new Scan();
            scan.addColumn(HBaseConstants.HBASE_COLUMN_FAMILY_URL_SEED.getBytes(), HBaseConstants.HBASE_COLUMN_QUALIFIER_URL.getBytes());
            try (ResultScanner scanner = table.getScanner(scan)) {
                for (Result result : scanner) {
                    byte[] value = result.getValue(HBaseConstants.HBASE_COLUMN_FAMILY_URL_SEED.getBytes(), HBaseConstants.HBASE_COLUMN_QUALIFIER_URL.getBytes());
                    if (value != null) {
                        return new String(value);
                    }
                }
            }
        } catch (IOException e) {
            LOGGER.error("从 HBase 获取 URL 时出错", e);
        }
        return null;
    }

    /**
     * 向 HBase 中添加 url
     * @param url
     */
    public static void addUrl(String url) {
        try (Table table = connection.getTable(TableName.valueOf(HBaseConstants.HBASE_TABLE_NAME))) {
            Put put = new Put(url.getBytes());
            // 添加 url 的逻辑
            put.addColumn(HBaseConstants.HBASE_COLUMN_FAMILY_URL_SEED.getBytes(), HBaseConstants.HBASE_COLUMN_QUALIFIER_URL.getBytes(), url.getBytes());
            table.put(put);
        } catch (IOException e) {
            LOGGER.error("向 HBase 添加 URL 时出错", e);
        }
    }

    /**
     * 关闭 HBase 连接
     */
    public static void closeConnection() {
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                LOGGER.error("关闭 HBase 连接时出错", e);
            }
        }
    }

    //检查数据是否存在
    public static boolean isTableExists() {
        try (Admin admin = connection.getAdmin()) {
            TableName tableName = TableName.valueOf(HBaseConstants.HBASE_TABLE_NAME);
            return admin.tableExists(tableName);
        } catch (IOException e) {
            LOGGER.error("检查表是否存在时出错", e);
            return false;
        }
    }

    /**
     * 检查数据是否存在
     * @param rowKey 行键
     * @param family 列族
     * @param qualifier 列限定符
     * @return 如果数据存在返回true，否则返回false
     */
//    public static boolean isDataExists(String rowKey, String family, String qualifier) {
//        try (Table table = connection.getTable(TableName.valueOf(HBaseConstants.HBASE_TABLE_NAME))) {
//            byte[] row = Bytes.toBytes(rowKey);
//            byte[] fam = Bytes.toBytes(family);
//            byte[] qual = Bytes.toBytes(qualifier);
//            Get get = new Get(row);
//            get.addColumn(fam, qual);
//            Result result = table.get(get);
//            return result.containsColumn(fam, qual);
//        } catch (IOException e) {
//            LOGGER.error("检查数据是否存在时出错", e);
//            return false;
//        }
//    }
//}

    /**
//     * 存储图片 URL 到 HBase
//     * @param imgUrl
     */
//    public static void storeImage(String imgUrl) {
//        try (Table table = connection.getTable(TableName.valueOf(HBaseConstants.HBASE_TABLE_NAME))) {
//            Put put = new Put(imgUrl.getBytes());
//            // 存储图片的逻辑
//            // 这里只是示例，需要根据实际情况实现具体逻辑
//            put.addColumn(HBaseConstants.HBASE_COLUMN_FAMILY_URL_SEED.getBytes(), "img_url".getBytes(), imgUrl.getBytes());
//            table.put(put);
//        } catch (IOException e) {
//            LOGGER.error("存储图片到 HBase 时出错", e);
//        }
//    }

    /**
     * 存储 Page 对象到 HBase
     * @param page
     */
//    public static void store(Page page) {
//        try (Table table = connection.getTable(TableName.valueOf(HBaseConstants.HBASE_TABLE_NAME))) {
//            List<Put> puts = new ArrayList<>();
//            byte[] rowKey = page.getId().getBytes(); //
//            byte[] cf1 = HBaseConstants.HBASE_COLUMN_FAMILY_URL_SEED.getBytes();
//            byte[] cf2 = HBaseConstants.HBASE_COLUMN_FAMILY_DOMAIN.getBytes();
//
//            // cf1:price
//            Put pricePut = new Put(rowKey);
//            // 必须要做是否为 null 判断，否则会有空指针异常
//            pricePut.addColumn(cf1, "price".getBytes(), page.getPrice() != null ? String.valueOf(page.getPrice()).getBytes() : "".getBytes());
//            puts.add(pricePut);
//
//            // cf1:comment
//            Put commentPut = new Put(rowKey);
//            commentPut.addColumn(cf1, "comment".getBytes(), page.getCommentCount() != null ? String.valueOf(page.getCommentCount()).getBytes() : "".getBytes());
//            puts.add(commentPut);
//
//            // cf1:brand
//            Put brandPut = new Put(rowKey);
//            brandPut.addColumn(cf1, "brand".getBytes(), page.getBrand() != null ? page.getBrand().getBytes() : "".getBytes());
//            puts.add(brandPut);
//
//            // cf1:url
//            Put urlPut = new Put(rowKey);
//            urlPut.addColumn(cf1, "url".getBytes(), page.getUrl() != null ? page.getUrl().getBytes() : "".getBytes());
//            puts.add(urlPut);
//
//            // cf2:title
//            Put titlePut = new Put(rowKey);
//            titlePut.addColumn(cf2, "title".getBytes(), page.getTitle() != null ? page.getTitle().getBytes() : "".getBytes());
//            puts.add(titlePut);
//
//            // cf2:params
//            Put paramsPut = new Put(rowKey);
//            paramsPut.addColumn(cf2, "params".getBytes(), page.getParams() != null ? page.getParams().getBytes() : "".getBytes());
//            puts.add(paramsPut);
//
//            // cf2:imgUrl
//            Put imgUrlPut = new Put(rowKey);
//            imgUrlPut.addColumn(cf2, "imgUrl".getBytes(), page.getImgUrl() != null ? page.getImgUrl().getBytes() : "".getBytes());
//            puts.add(imgUrlPut);
//
//            // 添加数据到表中
//            table.put(puts);
//        } catch (IOException e) {
//            LOGGER.error("存储 Page 对象到 HBase 时出错", e);
//        }
//    }
}