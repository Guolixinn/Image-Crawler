package cn.xpleaf.spider.core.pojo;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 图片对象，主要包含图片的相关数据
 */
public class Page implements Serializable {
    private String id;              // 图片唯一标
    private String url;           // url
    private String content;              // 网页内容
    private String imgUrl;          // 图片地址
    private byte[] imageData;       // 图片二进制数据
    private String imageSize;       // 图片尺寸，例如 1234*5678
    private String imageMemory;     // 图片内存大小
    private List<String> tags;            // 图片标签，多个标签用逗号分隔
    private List<String> urls = new ArrayList<>();  // 解析列表页面时用来保存解析的商品url的容器

    // 构造方法
    // public ImagePage() {}


    // Getter 和 Setter 方法
    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
    public String getUrl() {return url;}

    public void setUrl(String url) {
        this.url = url;
    }
    public String getImgUrl() {
        return imgUrl;
    }

    public void setImgUrl(String imgUrl) {
        this.imgUrl = imgUrl;
    }

    public byte[] getImageData() {
        return imageData;
    }

    public void setImageData(byte[] imageData) {
        this.imageData = imageData;
    }

    public String getImageSize() {
        return imageSize;
    }

    public void setImageSize(String imageSize) {
        this.imageSize = imageSize;
    }

    public String getImageMemory() {
        return imageMemory;
    }

    public void setImageMemory(String imageMemory) {
        this.imageMemory = imageMemory;
    }

    public List<String> getTags() {
        return tags;
    }

    public void setTags(List<String> tags) {
        this.tags = tags;
    }

    public List<String> getUrls() {
        return urls;
    }

    @Override
    public String toString() {
        return "ImagePage{" +
                "id='" + id + '\'' +
                "url='" + url + '\'' +
                ", imgUrl='" + imgUrl + '\'' +
                ", imageData=" + (imageData != null ? "[BINARY DATA]" : "null") +
                ", imageSize='" + imageSize + '\'' +
                ", imageMemory='" + imageMemory + '\'' +
                ", tags='" + tags + '\'' +
                '}';
    }
}
