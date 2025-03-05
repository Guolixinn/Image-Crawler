package cn.xpleaf.spider.core.store.impl;

import cn.xpleaf.spider.core.pojo.Page;
import cn.xpleaf.spider.core.store.IStore;
import cn.xpleaf.spider.utils.DBCPUtil;
import cn.xpleaf.spider.utils.ImageDownloader;
import org.apache.commons.dbutils.QueryRunner;
import cn.xpleaf.spider.client.MysqlClient;

import java.io.Serializable;
import java.sql.SQLException;
import java.io.IOException;
import java.net.URL;
import java.util.List;

/**
 * 使用dbc数据库连接池将数据写入mysql表中
 */
public class MySQLStoreImpl implements IStore, Serializable {
    //private QueryRunner queryRunner = new QueryRunner(DBCPUtil.getDataSource());

    @Override
    public void store(Page page) {
        String sql = "insert into image(id, img_url, image_data, image_size, image_memory, tags) values(?, ?, ?, ?, ?, ?)";
        // 创建 QueryRunner 实例
        QueryRunner queryRunner = new QueryRunner(DBCPUtil.getDataSource()); //通过 DBCPUtil.getDataSource() 获取数据源
        try {
            // 确保传入的参数数量和类型与 SQL 语句中的占位符匹配
            queryRunner.update(sql, page.getId(), page.getImgUrl(), null, null, null, null);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        String sql = "insert into image(id, img_url, image_data, image_size, image_memory, tags) values(?, ?, ?, ?, ?, ?)";
        QueryRunner queryRunner = new QueryRunner(DBCPUtil.getDataSource());
        try {
            // 确保传入的参数数量和类型与 SQL 语句中的占位符匹配
            queryRunner.update(sql, 1, "https://example.com/image.jpg", null, null, null, null);
//            queryRunner.update(sql, '1', '1', '1', '1', '1', '1', '1', '1', '1');
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
//    public void store(Page page) {
//        // 获取图片链接 URL
//        String imgUrl = page.getImgUrl();
//        if (imgUrl != null) {
//            try {
//                byte[] imageData = ImageDownloader.downloadImage(imgUrl);
//                // 补充缺失的参数
//                String category = "商品图"; // 假设图片分类为商品图
//                String tags = "图片标签"; // 假设图片标签
//                // 调用 MysqlClient 的 insert 方法插入数据
//                MysqlClient.insert(page, imageData, imgUrl, category, tags);
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
//    }
//}

//    public void store(Page page) {
//String sql = "insert into phone(id, source, brand, title, price, comment_count, url, img_url, params) values(?, ?, ?, ?, ?, ?, ?, ?, ?)";
//try {
//    queryRunner.update(sql, page.getId(),
//            page.getSource(),
//            page.getBrand(),
//            page.getTitle(),
//            page.getPrice(),
//            page.getCommentCount(),
//            page.getUrl(),
//            page.getImgUrl(),
//            page.getParams());
//} catch (SQLException e) {
//    e.printStackTrace();
//}
//    }

//    public static void main(String[] args) {
//        String sql = "insert into phone(id, source, brand, title, price, comment_count, url, img_url, params) values(?, ?, ?, ?, ?, ?, ?, ?, ?)";
//        QueryRunner queryRunner = new QueryRunner(DBCPUtil.getDataSource());
//        try {
//            queryRunner.update(sql, '1', '1', '1', '1', '1', '1', '1', '1', '1');
//        } catch (SQLException e) {
//            throw new RuntimeException(e);
//        }
//    }
//}