//package cn.xpleaf.spider.utils;
//
//import cn.xpleaf.spider.constants.SpiderConstants;
//import cn.xpleaf.spider.core.download.impl.HttpGetDownloadImpl;
//import org.apache.commons.io.FileUtils;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.hbase.HBaseConfiguration;
//import org.apache.hadoop.hbase.TableName;
//import org.apache.hadoop.hbase.client.*;
//import org.apache.hadoop.hbase.util.Bytes;
//import org.jsoup.Jsoup;
//import org.jsoup.nodes.Document;
//import org.jsoup.nodes.Element;
//import org.jsoup.select.Elements;
//import org.jsoup.Connection; // 明确导入 Jsoup 的 Connection 类
//import java.io.ByteArrayInputStream;
//import java.io.File;
//import java.io.IOException;
//import java.util.UUID;
//
//import java.io.Serializable;
//
//public class ImageCrawl implements Serializable {
//
//    private static ImageCrawl imageCrawl;
//    private static String url = "https://www.nipic.com/topic/show_29204_1.html";
//    private ImageDownloader downloader;
////    // HBase 表名
////    private static final String TABLE_NAME = "image_urls";
////    // 列族名
////    private static final String COLUMN_FAMILY = "url_info";
////    // 列限定符名
////    private static final String COLUMN_QUALIFIER = "image_url";
//
//    private ImageCrawl() {
//        // 初始化 HBase 连接
//        initHBase();
//        // 初始化下载器
//        this.downloader = new HttpGetDownloadImpl();
//    }
//
//    public static synchronized ImageCrawl getInstance() {
//        if (imageCrawl == null) {
//            imageCrawl = new ImageCrawl();
//            imageCrawl.jsoup(url);
//        }
//        return imageCrawl;
//    }
//
//    private void initHBase() {
//        try {
//            SpiderConstants spiderConstants = new SpiderConstants() {};
//            spiderConstants.initHBaseTable();
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//    }
//
////    private void initHBase() {
////        try {
////            Configuration config = HBaseConfiguration.create();
////            // 使用完整类名避免命名冲突
////            org.apache.hadoop.hbase.client.Connection connection = ConnectionFactory.createConnection(config);
////            try (Admin admin = connection.getAdmin()) {
////                TableName tableName = TableName.valueOf(SpiderConstants.TABLE_NAME);
////                if (!admin.tableExists(tableName)) {
////                    // 使用 HBase 1.1.5 兼容的 API 创建表和列族
////                    HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
////                    HColumnDescriptor columnFamilyDescriptor = new HColumnDescriptor(SpiderConstants.COLUMN_FAMILY_URL_SEED);
////                    tableDescriptor.addFamily(columnFamilyDescriptor);
////                    admin.createTable(tableDescriptor);
////                }
////            } finally {
////                if (connection != null) {
////                    connection.close();
////                }
////            }
////        } catch (IOException e) {
////            throw new RuntimeException(e);
////        }
////    }
//
//    private void jsoup(String url) {
//        try {
//            Document document = Jsoup.connect(url).get();
//            Elements select = document.select("li.new-search-works-item");
//            Configuration config = HBaseConfiguration.create();
//            // 使用完整类名避免命名冲突
//            org.apache.hadoop.hbase.client.Connection connection = ConnectionFactory.createConnection(config);
//            try (Table table = connection.getTable(TableName.valueOf(SpiderConstants.TABLE_NAME))) {
//                for (Element element : select) {
//                    Elements imgElement = element.select("a > img");
//                    String imgUrl = imgElement.attr("src");
//                    if (imgUrl.startsWith("//")) {
//                        imgUrl = "https:" + imgUrl;
//                    }
//                    // 使用 Jsoup 的 Connection 类
//                    Connection.Response response = Jsoup.connect(imgUrl).userAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36").ignoreContentType(true).execute();
//                    String fileName = System.currentTimeMillis() + "-" + UUID.randomUUID().toString().substring(10);
//                    FileUtils.copyInputStreamToFile(new ByteArrayInputStream(response.bodyAsBytes()), new File("/Volumes/GLXpassport/crawler_project/result/" + fileName + ".png"));
//
//                    // 存储 URL 到 HBase
//                    Put put = new Put(Bytes.toBytes(imgUrl.hashCode()));
//                    put.addColumn(Bytes.toBytes(SpiderConstants.COLUMN_FAMILY_URL_SEED), Bytes.toBytes(SpiderConstants.COLUMN_QUALIFIER_URL), Bytes.toBytes(imgUrl));
//                    table.put(put);
//                }
//            } finally {
//                if (connection != null) {
//                    connection.close();
//                }
//            }
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//    }
//
//    /**
//     * 根据传入的 URL 进行图片爬取
//     * @param url 图片所在页面的 URL
//     */
//    public void jsoupWithUrl(String url) {
//        jsoup(url);
//    }
//
//    /**
//     * 设置下载器
//     * @param downloader 下载器实例
//     */
//    public void setDownloader(ImageDownloader downloader) {
//        this.downloader = downloader;
//    }
//}