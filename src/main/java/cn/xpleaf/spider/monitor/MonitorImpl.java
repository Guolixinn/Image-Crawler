package cn.xpleaf.spider.monitor;

import java.util.concurrent.atomic.AtomicInteger;

public class MonitorImpl implements IMonitor {
    private boolean isMonitoring = false;
    private AtomicInteger processedUrls = new AtomicInteger(0);

    @Override
    public void startMonitoring() {
        this.isMonitoring = true;
        System.out.println("监控已启动");
    }

    @Override
    public void stopMonitoring() {
        this.isMonitoring = false;
        System.out.println("监控已停止");
    }

    @Override
    public boolean isMonitoring() {
        return this.isMonitoring;
    }

    @Override
    public String getDetailedMonitoringInfo() {
        return "已处理的URL数量: " + processedUrls.get();
    }

    public void incrementProcessedUrls() {
        processedUrls.incrementAndGet();
    }
}