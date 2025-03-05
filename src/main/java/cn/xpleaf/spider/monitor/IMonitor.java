package cn.xpleaf.spider.monitor;

public interface IMonitor {
    // 启动监控
    void startMonitoring();
    // 停止监控
    void stopMonitoring();
    // 检查是否正在监控
    boolean isMonitoring();
    // 获取详细监控信息
    String getDetailedMonitoringInfo();
}