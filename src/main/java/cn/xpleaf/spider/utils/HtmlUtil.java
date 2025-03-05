package cn.xpleaf.spider.utils;

import org.htmlcleaner.TagNode;
import org.htmlcleaner.XPatherException;

import java.util.ArrayList;
import java.util.List;

/**
 * 解析页面的工具类
 */
public class HtmlUtil {

    /**
     * 根据指定的xpath，从tagNode中选择具体的Dimensions
     */
    public static String getDimensions(TagNode tagNode, String xpath) throws Exception {
        // 根据传入的 XPath 选择节点
        Object[] resultNodes = tagNode.evaluateXPath(xpath);
        if (resultNodes != null && resultNodes.length > 0) {
            // 假设取第一个匹配的元素
            TagNode dimensionNode = (TagNode) resultNodes[0];
            return dimensionNode.getText().toString().trim();
        }
        return "";
    }
    /**
     * 通过url获取商品ID
     * https://item.jd.com/3133843.html
     * ==> 3133843
     *
     * @param url
     * @return
     */
    public static String getIdByUrl(String url) {
        String id = url.substring(url.lastIndexOf("/") + 1, url.lastIndexOf("."));
        return id;
    }

    /**
     * 根据指定的xpath，从tagNode中选择具体的标签Text
     *
     * @param tagNode
     * @param xpath
     * @return
     */
    public static String getTextByXpath(TagNode tagNode, String xpath) {
        Object[] objs = null;
        try {
            objs = tagNode.evaluateXPath(xpath);
            if (objs != null && objs.length > 0) {
                TagNode titleNode = (TagNode) objs[0];
                return titleNode.getText().toString().trim();
            }
        } catch (XPatherException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 根据xpath和属性获取对应标签的属性值
     *
     * @param tagNode
     * @param attr
     * @param xpath
     * @return
     */
    public static String getAttrByXpath(TagNode tagNode, String attr, String xpath) {
        try {
            Object[] objs = tagNode.evaluateXPath(xpath);
            if (objs != null && objs.length > 0) {
                TagNode node = (TagNode) objs[0];
                return node.getAttributeByName(attr);
            }
        } catch (XPatherException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 得到url列表
     * @param tagNode
//     * @param attr
//     * @param xpath
     * @return
     */
    public static List<String> getListUrlByXpath(TagNode tagNode){
        List<String> urls = new ArrayList<>();
        try {
            // XPath 查询：匹配 <a itemprop="url" href="...">
            String xpathExpression = "//a[@itemprop='url']/@href";  // 针对图片网站设计的Url的List读取方式。
            Object[] results = tagNode.evaluateXPath(xpathExpression);

            // 提取 href URL
            for (Object result : results) {
                urls.add(result.toString());
            }
            return urls;
        } catch (XPatherException e) {
            System.out.println("❌ XPath 解析错误：" + e.getMessage());
        }
        return null;
    }

    //获取图片对应的url
    public static String getImgUrlByXpath(TagNode tagNode) {
        String xpathExpression = "//*[@id=\"i\"]";

        try {
            Object[] objs = tagNode.evaluateXPath(xpathExpression);
            if (objs != null && objs.length > 0) {

                return titleNode.getText().toString().trim();
            }
        } catch (XPatherException e) {
            e.printStackTrace();
        }
        return null;
    }
    
    // 获取图片对应的标签内容
    public static List<String> getImgTagsByXpath(TagNode tagNode) {
        List<String> tagTexts = new ArrayList<>();
        try {
            // XPath 查询：匹配 <a rel="tag">，提取文本内容
            String xpathExpression = "//a[@rel='tag']";
            Object[] results = tagNode.evaluateXPath(xpathExpression);

            for (Object result : results) {
                if (result instanceof TagNode) {
                    tagTexts.add(((TagNode) result).getText().toString().trim());
                }
            }
            return tagTexts;
        } catch (XPatherException e) {
            System.out.println("❌ XPath 解析错误：" + e.getMessage());
        }
        return null;
    }
    // 提取内存大小
    public static String extractElementTextByXPath(TagNode tagNode) {
        try {
            // 执行 XPath 查询
            String xpathExpression = "//*[@id='main']/div[2]/div[2]/ul[2]/li[2]/span[2]";
            Object[] results = tagNode.evaluateXPath(xpathExpression);

            if (results.length > 0 && results[0] instanceof TagNode) {
                return ((TagNode) results[0]).getText().toString().trim();
            }
        } catch (XPatherException e) {
            System.out.println("❌ XPath 解析错误：" + e.getMessage());
        }
        return "";
    }
    // 提取 id="s" 元素的文本内容(在该网页中，该内容等价于分辨率大小)
    public static String getImgSizeByXpath(TagNode tagNode, String elementId) {
        try {
            // XPath 查询：匹配 id="s" 的元素
            String xpathExpression = "//*[@id='" + elementId + "']";
            Object[] results = tagNode.evaluateXPath(xpathExpression);

            if (results.length > 0 && results[0] instanceof TagNode) {
                return ((TagNode) results[0]).getText().toString().trim();
            }
        } catch (XPatherException e) {
            System.out.println("❌ XPath 解析错误：" + e.getMessage());
        }
        return "";
    }


}
