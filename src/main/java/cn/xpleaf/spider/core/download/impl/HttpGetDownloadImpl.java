package cn.xpleaf.spider.core.download.impl;

import cn.xpleaf.spider.core.download.IDownload;
import cn.xpleaf.spider.core.pojo.Page;
import cn.xpleaf.spider.utils.HttpUtil;
import org.apache.http.HttpHost;

import java.awt.font.ImageGraphicAttribute;
import java.io.Serializable;

import cn.xpleaf.spider.utils.ImageDownloader;
import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.UUID;

import org.apache.commons.io.FileUtils;

/**
 * 数据下载实现类
 */


//public class HttpGetDownloadImpl implements IDownload,Serializable {
//
//    private static final long serialVersionUID = 1L;
//
//    // 新增：请求头
//    private Map<String, String> headers = new HashMap<>();
//
//    // 新增：设置请求头的方法
//    public void setHeaders(Map<String, String> headers) {
//        this.headers = headers;
//    }
//
//    @Override
//    public Page download(String url) {
//        Page page = new Page();
//        page.setUrl(url);
//
//        // 调用 HttpUtil.getRandomProxy() 方法获取随机代理
//        HttpHost proxy = HttpUtil.getRandomProxy();
//
//        CloseableHttpClient httpClient = null;
//        HttpGet httpGet = new HttpGet(url);
//
//        // 新增：设置请求头
//        for (Map.Entry<String, String> entry : headers.entrySet()) {
//            httpGet.setHeader(entry.getKey(), entry.getValue());
//        }
//
//        try {
//            if (proxy != null) {
//                // 创建 RequestConfig 对象，并设置代理
//                RequestConfig config = RequestConfig.custom()
//                        .setProxy(proxy)
//                        .setConnectTimeout(5000) // 设置连接超时时间，单位毫秒
//                        .setConnectionRequestTimeout(1000) // 设置从connect Manager获取Connection 超时时间，单位毫秒
//                        .setSocketTimeout(5000) // 请求获取数据的超时时间，单位毫秒
//                        .build();
//                // 将配置对象设置到 HTTP 请求对象中
//                httpGet.setConfig(config);
//            }
//
//            // 创建 HttpClient 对象
//            if (proxy != null) {
//                httpClient = HttpClients.custom().setProxy(proxy).build();
//            } else {
//                httpClient = HttpClients.createDefault();
//            }
//
//            // 执行请求
//            CloseableHttpResponse response = httpClient.execute(httpGet);
//            if (response.getStatusLine().getStatusCode() == 200) {
//                String content = EntityUtils.toString(response.getEntity(), "UTF-8");
//                page.setContent(content);
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        } finally {
//            if (httpClient != null) {
//                try {
//                    httpClient.close();
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//            }
//        }
//
//        return page;
//    }
//
//
//    public void downloadImage(String imgUrl, String filePath) {
//        try {
//            // 处理图片URL
//            if (imgUrl.startsWith("//")) {
//                imgUrl = "https:" + imgUrl;
//            }
//            // 使用 Jsoup 的 Connection 类
//            Connection.Response response = Jsoup.connect(imgUrl).userAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36").ignoreContentType(true).execute();
//            String fileName = System.currentTimeMillis() + "-" + UUID.randomUUID().toString().substring(10);
//            FileUtils.copyInputStreamToFile(new ByteArrayInputStream(response.bodyAsBytes()), new File(filePath + fileName + ".png"));
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//    }
//}

public class HttpGetDownloadImpl implements IDownload, Serializable {

    @Override
    public Page download(String url) {
        Page page = new Page();
        String content = HttpUtil.getHttpContent(url);  // 获取网页数据
        page.setUrl(url);
        page.setContent(content);
        return page;
    }
}
