package cn.xpleaf.spider.core.pojo;

import java.io.Serializable;
import java.util.List;
import java.util.Map;


public class UrlList implements Serializable {

    private List<String> urlList;

//    public UrlList(Map<String, Integer> urlPriorityMap) {
//        this.urlPriorityMap = urlPriorityMap;
//    }




    public List<String> getList() {
        return urlList;
    }

    public void setList(List<String> urlList) {
        this.urlList = urlList;
    }
}
