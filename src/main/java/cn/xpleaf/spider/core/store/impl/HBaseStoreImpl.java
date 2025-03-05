//package cn.xpleaf.spider.core.store.impl;
//
//import cn.xpleaf.spider.core.pojo.Page;
//import cn.xpleaf.spider.core.store.IStore;
//import cn.xpleaf.spider.utils.HBaseUtil;
//import cn.xpleaf.spider.utils.ImageCrawl;
//
///**
// * 将爬虫解析之后的数据存储到HBase对应的表product中
// cf1 存储 id img_url image_data
// cf2 存储 category tags
// */
//
//public class HBaseStoreImpl implements IStore {
//    @Override
//    public void store(Page page) {
//
//        HBaseUtil.store(page);
//
//        // 图片爬取和存储逻辑
//        String imgUrl = page.getImgUrl();
//        if (imgUrl != null) {
//            // 使用ImageCrawl类进行图片爬取
//            cn.xpleaf.spider.utils.ImageCrawl imageCrawl = cn.xpleaf.spider.utils.ImageCrawl.getInstance();
//            imageCrawl.jsoupWithUrl(imgUrl);
//            // 存储图片到HBase
//            HBaseUtil.storeImage(imgUrl);
//            // 存储图片到MySQL，需要实现相关逻辑
//            // 这里由于ImageCrawl类没有返回图片数据，暂时无法实现存储到MySQL
//            // storeImageToMySQL(imgUrl, imageData);
//        }
//    }
//
//    /**
//     * 下载图片
//     * @param imgUrl 图片URL
//     * @return 图片数据
//     */
//    private byte[] downloadImage(String imgUrl) {
//        // 实现图片下载逻辑
//        return null;
//    }
//
//    /**
//     * 存储图片到MySQL
//     * @param imgUrl 图片URL
//     * @param imageData 图片数据
//     */
//    private void storeImageToMySQL(String imgUrl, byte[] imageData) {
//        // 实现存储图片到MySQL的逻辑
//    }
//}
