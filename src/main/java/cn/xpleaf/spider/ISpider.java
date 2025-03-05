package cn.xpleaf.spider;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import cn.xpleaf.spider.client.MysqlClient;
import cn.xpleaf.spider.constants.SpiderConstants;
import cn.xpleaf.spider.core.download.IDownload;
import cn.xpleaf.spider.core.download.impl.HttpGetDownloadImpl;
//import cn.xpleaf.spider.utils.ImageCrawl;
import cn.xpleaf.spider.core.parser.IParser;
import cn.xpleaf.spider.core.parser.Impl.HippopxHtmlParserImpl;
import cn.xpleaf.spider.core.pojo.Page;
import cn.xpleaf.spider.core.pojo.UrlList;
import cn.xpleaf.spider.core.repository.IRepository;
import cn.xpleaf.spider.core.repository.impl.RandomHBaseRepositoryImpl;
import cn.xpleaf.spider.core.store.IStore;
import cn.xpleaf.spider.core.store.impl.MySQLStoreImpl;
//import cn.xpleaf.spider.utils.HBaseUtil;
import cn.xpleaf.spider.utils.ImageDownloader;
import cn.xpleaf.spider.utils.SpiderUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
//import redis.clients.jedis.Jedis;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;
import java.nio.charset.Charset;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * 爬虫入口类
 */
public class ISpider implements Serializable {
    // log4j日志记录
    private Logger logger = LoggerFactory.getLogger(ISpider.class);
    // 爬虫下载器
    private IDownload download;
    // 爬虫解释器map: key-->需要爬取的网页的顶级域名     value-->该顶级域名的解释器实现类对象
    private Map<String, IParser> parsers = new HashMap<>();
    // 爬虫存储器
    private IStore store;
    // 域名高低优先级url标识器map: key-->domain   value-->Map<Level, url> 其中level为higher或lower即高低优先级的意思
    private Map<String, Map<String, String>> urlLevelMarker = new HashMap<>();
    // 域名列表
    private List<String> domains = new ArrayList<>();
    // 种子url
    /**
     * 这是初始启动爬虫程序时的种子url，当将该种子url的数据爬取完成后就没有数据爬取了
     * 那如何解决呢？这就需要使用我们的url调度系统，我们另外启动了一个url调度程序
     * 该程序会定时从redis的种子url列表中获取种子url，然后再添加到高优先级url列表中
     * 这样我们的爬虫程序就不会停下来，达到了定时爬取特别网页数据的目的
     */
    private List<String> seedUrls = new ArrayList<>();
    // url仓库
    private IRepository repository;

    // 新增：爬虫开始时间
    private long startTime;
    // 新增：爬虫爬取的页面数量
    private int processedPageCount;

    // 新增：随机数生成器，用于生成随机的请求间隔时间
    private Random random = new Random();

    //单例模式
    private static ISpider iSpider;
    private ISpider(){}

    public static synchronized ISpider getInstance(){
        if(iSpider == null){
            iSpider = new ISpider();
            // 1.注入下载器
            iSpider.setDownload(new HttpGetDownloadImpl());
            // 2.注入解析器
            // 2.1 添加需要爬取的域名列表
            List<String> domains = new ArrayList<>();
            domains.add("hippopx.com");
//            iSpider.setDomains(domains);
            // 2.2 注入解析器
            iSpider.setParsers("hippopx.com", new  HippopxHtmlParserImpl());

//            // 2.2 设置高低优先级url标识器
//            Map<String, String> jdMarker = new HashMap<>();
//            jdMarker.put("higher", "https://list.jd.com/");
//            jdMarker.put("lower", "https://item.jd.com");

//            iSpider.setUrlLevelMarker("jd.com", jdMarker);


            // 3.注入存储器
//            iSpider.setStore(new MySQLStoreImpl());

            // 4.设置种子url
            iSpider.setSeedUrls("https://www.hippopx.com/en/gear-mechanics-machine-curb-life-speed-technology-81362");
            // 5.设置url仓库
            //RandomRedisRepositoryImpl repository = new RandomRedisRepositoryImpl();
            iSpider.setRepository(new RandomHBaseRepositoryImpl()); // 设置url仓库
            // 新增：记录爬虫开始时间
            iSpider.startTime = System.currentTimeMillis();
            iSpider.processedPageCount = 0;
        }
        return iSpider;
    }
    /**
     * 完成网页数据的下载
     *
     * @param url
     * @return
     */
//    public Page download(String url) {
//        // 新增：在下载之前，随机休眠一段时间，模拟人类的浏览行为
//        int sleepTime = random.nextInt(2000) + 1000; // 随机休眠1-3秒
//        try {
//            Thread.sleep(sleepTime);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//
////        // 新增：设置请求头，模拟浏览器行为
////        Map<String, String> headers = new HashMap<>();
////        headers.put("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36");
////        headers.put("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9");
////        headers.put("Accept-Language", "zh-CN,zh;q=0.9");
////
////        // 假设IDownload接口有一个设置请求头的方法，需要根据实际情况修改
////        if (download instanceof HttpGetDownloadImpl) {
////            ((HttpGetDownloadImpl) download).setHeaders(headers);
////        }
//
//        return this.download.download(url);
//    }

    /**
     * 根据不同的域名，选择不同的解析器对下载的网页进行解析
     *
     * @param page
     */
    public void parser(Page page, String domain) {
        IParser domainParser = this.parsers.get(domain);
        if (domainParser != null) {
            domainParser.parser(page);
        } else {
            logger.error("没有对应域名{}的解析器", domain);
        }
    }

    /**
     * 存储解析之后的网页数据信息
     *
     * @param page
     */
    public void store(Page page) {
        this.store.store(page);
    }

    public  UrlList startSingle(String url){
        UrlList urlList = new UrlList();
        String domain = SpiderUtil.getTopDomain(url);   // 获取url对应的顶级域名
        System.out.println("-----flink url"+"    "+url);
        if (url != null) {  // 从url仓库中获取的url不为null
            // 新增：使用布隆过滤器检查 URL 是否已经解析过
            if (bloomFilter.mightContain(url)) {
                logger.info("URL {} 已经解析过，跳过", url);
                return urlList;
            }

            // 下载网页
            Page page = download.download(url);
            // 解析网页
            if (page.getContent() != null) { // 只有content不为null时才进行后面的操作，否则没有意义
                List<String> newList = new ArrayList<>();
//                List<String> lowList = new ArrayList<>();
                parser(page, domain); // 如果该url为列表url，从这里有可能解析出很多的url
                for (String pUrl : page.getUrls()) { // 向url仓库中添加url
                    logger.info(pUrl);
//                    String higherUrlMark = urlLevelMarker.get(domain).get("higher");
////                    String lowerUrlMark = urlLevelMarker.get(domain).get("lower");
//                    if (pUrl.startsWith(higherUrlMark)) {    // 高优先级
//                        //repository.offerHigher(pUrl);
//                        List.add(pUrl);
//                    } else if (pUrl.startsWith(lowerUrlMark)) { // 低优先级
//                        //repository.offerLower(pUrl);
                        newList.add(pUrl);
                }
                urlList.setList(newList);


                //唤醒
            //
                if (page.getId() != null) {  // 当商品id不为null时，说明前面解析的url是商品url，而不是列表url，这时存储数据才有意义
                    // 存储解析数据
                    // store(page);
                    // System.out.println(page);
                    MysqlClient.insert(page);
                }

                // 新增：处理页面数量加1
                processedPageCount++;

                // 将 URL 添加到布隆过滤器中
                bloomFilter.put(url);
            }
//                    try {
//                        // 获取图片链接
//                        String imgUrl = page.getImgUrl();
//                        if (imgUrl != null) {
//                            byte[] imageData = ImageDownloader.downloadImage(imgUrl);
//                            // 补充缺失的参数
//                            String category = "商品图"; // 假设图片分类为商品图
//                            String tags = "图片标签"; // 假设图片标签
//                            // 调用 MysqlClient 的 insert 方法插入数据
//                            MysqlClient.insert(page, imageData, imgUrl, category, tags);
//                        }
//                    } catch (IOException e) {
//                        e.printStackTrace();
//                    }
//                }

            // 上面操作结束之后必须要休息一会，否则频率太高的话很有可能会被封ip
            //SpiderUtil.sleep(1000);
        } else {    // 从url仓库中没有获取到url
            logger.info("没有url，请及时添加种子url");
            SpiderUtil.sleep(2000);
        }

        return urlList;
    }


    public void setLogger(Logger logger) {
        this.logger = logger;
    }

    public void setDownload(HttpGetDownloadImpl download) {
        this.download = download;
    }

    /**
     * 添加网页解析器
     *
     * @param domain 域名
     * @param parser 该域名对应的具体解析器实现类对象
     */
    public void setParsers(String domain, IParser parser) {
        this.parsers.put(domain, parser);
    }

//    public void setStore(IStore store) {
//        this.store = store;
//    }

    public void setSeedUrls(String seedUrl) {
        this.seedUrls.add(seedUrl);
    }

    public void setRepository(IRepository repository) {
        this.repository = repository;
        for (String seedUrl : this.seedUrls) {   // 添加种子url到url仓库中
            this.repository.offer(seedUrl);
        }
    }

    /**
     * 设置域名高低优先级url标识器
     *
     * @param domain 域名
     * @param map    域名的高低优先级url解析器Map<level, url>
     */
    public void setUrlLevelMarker(String domain, Map<String, String> map) {
        this.urlLevelMarker.put(domain, map);
    }

//    public void setDomains(List<String> domains) {
//        this.domains = domains;
//        // 同时，为了方便操作，也将域名添加到 HBase 的 spider:website_domains 表中，这样就不用手动添加
//        Configuration config = HBaseConfiguration.create();
//        try (Connection connection = ConnectionFactory.createConnection(config);
//             Table table = connection.getTable(TableName.valueOf("spider:website_domains"))) {
//
//            // 删除原有数据（如果需要）
//            Delete delete = new Delete(Bytes.toBytes(SpiderConstants.SPIDER_WEBSITE_DOMAINS_KEY));
//            table.delete(delete);
//
//            // 插入新的域名数据
//            for (String domain : domains) {
//                Put put = new Put(Bytes.toBytes(SpiderConstants.SPIDER_WEBSITE_DOMAINS_KEY));
//                put.addColumn(Bytes.toBytes("domains"), Bytes.toBytes(domain), Bytes.toBytes(domain));
//                table.put(put);
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }

    // 新增：获取爬虫运行时间（毫秒）
    public long getRunningTime () {
        return System.currentTimeMillis() - startTime;
    }

    // 新增：获取爬虫爬取的页面数量
    public int getProcessedPageCount () {
        return processedPageCount;
    }
}
//    public void setDomains(List<String> domains) {
//        this.domains = domains;
//        // 同时，为了方便操作，也将域名添加到redis的spider.website.domains集合中，这样就不用手动在redis中添加
//        Jedis jedis = JedisUtil.getJedis();
//        jedis.del(SpiderConstants.SPIDER_WEBSITE_DOMAINS_KEY);
//        for(String domain : domains) {
//            jedis.sadd(SpiderConstants.SPIDER_WEBSITE_DOMAINS_KEY, domain);
//        }
//        JedisUtil.returnJedis(jedis);
//    }

