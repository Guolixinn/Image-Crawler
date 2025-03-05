package cn.xpleaf.spider.hbase;

import cn.xpleaf.spider.constants.SpiderConstants;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static cn.xpleaf.spider.utils.SpiderUtil.getTopDomain;

/**
 * 该类实现了 SourceFunction<String> 接口，作为 Apache Flink 流处理中的一个数据源，从 HBase 中读取数据。
 */
public class MyHBaseSource implements SourceFunction<String> {
    private boolean isRunning = true; //isRunning 用于控制数据读取的循环
    private Connection connection = null; //connection 是 HBase 的连接对象
    private Table table = null; // table 是 HBase 的表对象
    private final long SLEEP_MILLION = 5000; // SLEEP_MILLION 是每次读取数据后的延迟时间



    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        Configuration config = HBaseConfiguration.create();
        try {
            // 创建 HBase 连接
            connection = ConnectionFactory.createConnection(config);
            // 获取表对象
            table = connection.getTable(TableName.valueOf(SpiderConstants.TABLE_NAME));

            // **检查 HBase 是否为空，若为空则插入初始 URL**
            if (isHBaseEmpty(table)) {
                insertSeedUrls(table);
            }

            while (isRunning) {
                String url = null;
                while (url == null) {
                    // **从 HBase 获取数据**
                    Scan scan = new Scan();
                    scan.addColumn(Bytes.toBytes(SpiderConstants.COLUMN_FAMILY_PRIORITY), Bytes.toBytes(SpiderConstants.COLUMN_QUALIFIER_PRIORITY_VALUE));
                    scan.addColumn(Bytes.toBytes(SpiderConstants.COLUMN_FAMILY_URL_SEED), Bytes.toBytes(SpiderConstants.COLUMN_QUALIFIER_URL));
                    ResultScanner scanner = table.getScanner(scan);
//                    List<String> urls = new ArrayList<>();
//                    for (Result result : scanner) {
//                        byte[] priorityValue = result.getValue(Bytes.toBytes(SpiderConstants.COLUMN_FAMILY_PRIORITY), Bytes.toBytes(SpiderConstants.COLUMN_QUALIFIER_PRIORITY_VALUE));
//                        if (priorityValue != null && !Bytes.toBoolean(priorityValue)) {
//                            byte[] urlValue = result.getValue(Bytes.toBytes(SpiderConstants.COLUMN_FAMILY_URL_SEED), Bytes.toBytes(SpiderConstants.COLUMN_QUALIFIER_URL));
//                            if (urlValue != null) {
//                                urls.add(Bytes.toString(urlValue));
//                            }
//                        }
//                    }
                    try {
                        for (Result result : scanner) {
                            byte[] priorityValue = result.getValue(Bytes.toBytes(SpiderConstants.COLUMN_FAMILY_PRIORITY), Bytes.toBytes(SpiderConstants.COLUMN_QUALIFIER_PRIORITY_VALUE));
                            if (priorityValue != null && !Bytes.toBoolean(priorityValue)) {
                                byte[] urlValue = result.getValue(Bytes.toBytes(SpiderConstants.COLUMN_FAMILY_URL_SEED), Bytes.toBytes(SpiderConstants.COLUMN_QUALIFIER_URL));
                                if (urlValue != null) {
                                    url = Bytes.toString(urlValue);
                                    sourceContext.collect(url);
                                    // 标记该 URL 已解析
                                    Put put = new Put(result.getRow());
                                    put.addColumn(Bytes.toBytes(SpiderConstants.COLUMN_FAMILY_PRIORITY), Bytes.toBytes(SpiderConstants.COLUMN_QUALIFIER_PRIORITY_VALUE), Bytes.toBytes(true));
                                    table.put(put);
                                    break; // 找到第一个符合条件的就退出循环
                                }
                            }
                        }
                    } finally {
                        scanner.close();
                    }
                    // 模拟延迟
                    Thread.sleep(SLEEP_MILLION);
                }

                // 输出获取到的 URL
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (table != null) {
                table.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
    }

    private boolean isHBaseEmpty(Table table) throws IOException {
        Scan scan = new Scan();
        scan.setCaching(1); // 只取 1 条数据检查是否为空
        try (ResultScanner scanner = table.getScanner(scan)) {
            return !scanner.iterator().hasNext();
        }
    }

    /**
     * **插入初始种子 URL**
     */
    private void insertSeedUrls(Table table) throws IOException {
        String[] seedUrls = {
                "https://www.hippopx.com/en/query?q=fruit",

        };

        List<Put> puts = new ArrayList<>();
        for (String url : seedUrls) {
            Put put = new Put(Bytes.toBytes(url.hashCode()));
            put.addColumn(Bytes.toBytes(SpiderConstants.COLUMN_FAMILY_URL_SEED), Bytes.toBytes(SpiderConstants.COLUMN_QUALIFIER_URL), Bytes.toBytes(url));
            put.addColumn(Bytes.toBytes(SpiderConstants.COLUMN_FAMILY_DOMAIN), Bytes.toBytes(SpiderConstants.COLUMN_QUALIFIER_DOMAIN_NAME ), Bytes.toBytes(getTopDomain(url)));
            System.out.println(getTopDomain(url));
            put.addColumn(Bytes.toBytes(SpiderConstants.COLUMN_FAMILY_PRIORITY), Bytes.toBytes(SpiderConstants.COLUMN_QUALIFIER_PRIORITY_VALUE ), Bytes.toBytes(false));
            puts.add(put);
        }

        table.put(puts);
        System.out.println("✅ 已插入初始种子 URL 到 HBase");
    }

//    @Override
//    public void run(SourceContext<String> sourceContext) throws Exception {
//        Configuration config = HBaseConfiguration.create();
//        try {
//            // 创建 HBase 连接
//            connection = ConnectionFactory.createConnection(config);
//            // 获取表对象
//            table = connection.getTable(TableName.valueOf(SpiderConstants.TABLE_NAME));
//
//            if (isHBaseEmpty(table)) {
//                insertSeedUrls(table);
//            }
//
//
//            while (isRunning) {
//                String url = null;
//                while (url == null) {
//                    // 模拟从 HBase 中获取数据，这里简单使用 Scan 扫描表
//                    Scan scan = new Scan();
//                    scan.addColumn(Bytes.toBytes(SpiderConstants.COLUMN_FAMILY_PRIORITY), Bytes.toBytes(SpiderConstants.COLUMN_QUALIFIER_PRIORITY_VALUE));
//                    scan.addColumn(Bytes.toBytes(SpiderConstants.COLUMN_FAMILY_URL_SEED), Bytes.toBytes(SpiderConstants.COLUMN_QUALIFIER_URL));
//                    ResultScanner scanner = table.getScanner(scan);
//                    List<String> urls = new ArrayList<>();
//                    for (Result result : scanner) {
//                        byte[] priorityValue = result.getValue(Bytes.toBytes(SpiderConstants.COLUMN_FAMILY_PRIORITY), Bytes.toBytes(SpiderConstants.COLUMN_QUALIFIER_PRIORITY_VALUE));
//                        if (priorityValue != null &&!Bytes.toBoolean(priorityValue)) {
//                            byte[] urlValue = result.getValue(Bytes.toBytes(SpiderConstants.COLUMN_FAMILY_URL_SEED), Bytes.toBytes(SpiderConstants.COLUMN_QUALIFIER_URL));
//                            if (urlValue != null) {
//                                urls.add(Bytes.toString(urlValue));
//                            }
//                        }
//                    }
//                    scanner.close();
//
//                    if (!urls.isEmpty()) {
//                        url = urls.get(0);
//                        // 尝试加锁
//                        if (tryLockUrl(url)) {
//                            // 标记该 URL 已解析
//                            Put put = new Put(Bytes.toBytes(url.hashCode()));
//                            put.addColumn(Bytes.toBytes(SpiderConstants.COLUMN_FAMILY_PRIORITY), Bytes.toBytes(SpiderConstants.COLUMN_QUALIFIER_PRIORITY_VALUE), Bytes.toBytes(true));
//                            table.put(put);
//                        } else {
//                            url = null;
//                        }
//                    }
//
//                    // 模拟延迟
//                    Thread.sleep(SLEEP_MILLION);
//                }
//
//                // 输出获取到的 URL
//                sourceContext.collect(url);
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        } finally {
//            if (table != null) {
//                table.close();
//            }
//            if (connection != null) {
//                connection.close();
//            }
//        }
//    }

//    private boolean tryLockUrl(String url) throws IOException {
//        // 尝试添加锁标志，使用 checkAndPut 实现原子操作
//        Put put = new Put(Bytes.toBytes(url.hashCode()));
//        put.addColumn(Bytes.toBytes(SpiderConstants.COLUMN_FAMILY_LOCK), Bytes.toBytes(SpiderConstants.COLUMN_QUALIFIER_LOCK_VALUE), Bytes.toBytes(true));
//        return table.checkAndPut(Bytes.toBytes(url.hashCode()),
//                Bytes.toBytes(SpiderConstants.COLUMN_FAMILY_LOCK),
//                Bytes.toBytes(SpiderConstants.COLUMN_QUALIFIER_LOCK_VALUE),
//                null, put);
//    }

    @Override
    public void cancel() {
        isRunning = false;
        if (table != null) {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}