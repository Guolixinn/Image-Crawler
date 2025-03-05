package cn.xpleaf.spider.core.parser.Impl;

import cn.xpleaf.spider.core.parser.IParser;
import cn.xpleaf.spider.core.pojo.Page;
import cn.xpleaf.spider.utils.HtmlUtil;
import org.htmlcleaner.HtmlCleaner;
import org.htmlcleaner.TagNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.List;

import static cn.xpleaf.spider.utils.ImageDownloader.downloadImage;

// 修正类名拼写错误
public class HippopxHtmlParserImpl implements IParser, Serializable {
    // log4j日志记录，去掉多余的分号
    private Logger logger = LoggerFactory.getLogger(HippopxHtmlParserImpl.class);

    @Override
    public void parser(Page page) {
        HtmlCleaner cleaner = new HtmlCleaner();
        /**
         * cleaner.clean()方法，如果page.getContent为null，那么整个程序就会一直阻塞在这里
         * 所以，在前面的代码中ISpider.start()方法，下载网页后，需要对内容进行判断，如果content为空，则跳过解析
         */
        TagNode rootNode = cleaner.clean(page.getContent());  // 将存储在Page中的Content转化为TagNode形式的数据

        long start = System.currentTimeMillis();    // 解析开始时间
        // 进行判断 根据url的类型进行列表解析还是商品解析
        if (isValidHippopxURL(page.getUrl())) {   // 解析图片网站，获取图片url以及其标签、大小等信息
            parserImage(page, rootNode);

        } else {  // 非照片页，因此将其解析为网站列表
            List<String> urls = HtmlUtil.getListUrlByXpath(rootNode);  // 解析出网站列表

            page.getUrls().addAll(urls);  // 将网站内容加入到page中去
            logger.info("解析列表页面:{}, 消耗时长:{}ms", page.getImgUrl(), System.currentTimeMillis() - start);
            if (System.currentTimeMillis() - start == 0) {   // 解析图片数据页码数时，偶尔获取不到下一页，时间就为0ms，这时需要重试
                logger.info("解析列表页面:{}, 消耗时长:{}ms， 尝试将其重新添加到高优先级url队列中",  page.getImgUrl(), System.currentTimeMillis() - start);
            }

        }
    }

    private void parserImage(Page page, TagNode tagNode){
        // 1.获取该照片页的主要照片的url
        String imgUrl = HtmlUtil.getImgUrlByXpath(tagNode);
        page.setImgUrl(imgUrl);

        // 2.从照片的url中获取照片的唯一id
        page.setId(extractNumbers(imgUrl));

        // 3.从照片页中获取该图片对应的标签
        List<String> tags = HtmlUtil.getImgTagsByXpath(tagNode);
        page.setTags(tags);

        // 4.从照片页获取该图片对应的分辨率
        String Size = HtmlUtil.getImgSizeByXpath(tagNode, "s");
        page.setImageSize(Size);

        // 5.从照片页获取图片对应的内存大小
        String Memory = HtmlUtil.extractElementTextByXPath(tagNode);
        page.setImageMemory(Memory);

        byte[] imagedata = null;
        try {
            imagedata = downloadImage(imgUrl);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        page.setImageData(imagedata);
        // 去掉重复的代码
        // page.setImageMemory(Memory);
    }


    // 用于将URL中的唯一编码提取出来
    public static String extractNumbers(String url) {
        // 正则匹配三个连续的数字片段
        Pattern pattern = Pattern.compile("/(\\d+/\\d+/\\d+)/");
        Matcher matcher = pattern.matcher(url);

        if (matcher.find()) {
            return matcher.group(1);
        }
        return null;
    }


    // 用于判断该网页是否为图片页面
    public static boolean isValidHippopxURL(String url) {
        // 正则表达式匹配要求的 URL 结构
        String regex = "^https://www\\.hippopx\\.com/en/([a-zA-Z0-9-]+)-(\\d+)$";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(url);
        return matcher.matches();
    }
}