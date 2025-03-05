package cn.xpleaf.spider.core.store;

import cn.xpleaf.spider.core.pojo.Page;

/**
 * 网页图片数据的存储
 */
public interface IStore {
    /**
     * 存储包含网页图片信息的 Page 对象
     * @param page 包含网页图片信息的 Page 对象
     */
    public void store(Page page);
}