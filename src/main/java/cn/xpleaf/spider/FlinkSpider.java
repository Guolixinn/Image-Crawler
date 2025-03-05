package cn.xpleaf.spider;


import cn.xpleaf.spider.constants.SpiderConstants;
import cn.xpleaf.spider.core.pojo.UrlList;
import cn.xpleaf.spider.map.SpiderFlatMapFunction;
import cn.xpleaf.spider.hbase.MyHBaseSource;
import cn.xpleaf.spider.sink.MyHBaseSinkFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class FlinkSpider {
    public static void main(String[] args) throws Exception {
        ISpider iSpider = ISpider.getInstance();

        // 创建流式执行环境
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 添加数据源
        DataStreamSource<String> stream = env.addSource(new MyHBaseSource());

        // 自定义哈希分发
        KeySelector<String, String> urlKeySelector = new KeySelector<String, String>() {
            @Override
            public String getKey(String url) throws Exception {
                return url;
            }
        };

        // 使用 keyBy 进行哈希分发
        SingleOutputStreamOperator<UrlList> urlListSingleOutputStreamOperator = stream
                .keyBy(urlKeySelector)
                .flatMap(new SpiderFlatMapFunction(iSpider));

        urlListSingleOutputStreamOperator.addSink(new MyHBaseSinkFunction());
        env.execute();

        // 新增：流式计算结束后删除 HBase 数据
        deleteHBaseData();
    }

    private static void deleteHBaseData() {
        Configuration config = HBaseConfiguration.create();
        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {

            TableName tableName = TableName.valueOf(SpiderConstants.TABLE_NAME);
            if (admin.tableExists(tableName)) {
                // 禁用表
                admin.disableTable(tableName);
                // 删除表
                admin.deleteTable(tableName);
                System.out.println("✅ 已删除 HBase 表: " + tableName.getNameAsString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
