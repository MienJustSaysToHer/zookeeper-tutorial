package com.heyuhuan.worker.zookeeper;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ZKConnectSessionWatcher implements Watcher {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZKConnectSessionWatcher.class);

    private static final String ZK_SERVER_PATH = "127.0.0.1:2181";
    private static final Integer TIMEOUT = 5000;

    public static void main(String[] args) throws InterruptedException, IOException {
        ZooKeeper zooKeeper = new ZooKeeper(ZK_SERVER_PATH, TIMEOUT, new ZKConnectSessionWatcher());

        LOGGER.debug("开始连接");
        LOGGER.debug("连接状态：{}", zooKeeper.getState());

        Thread.sleep(2000);

        long sessionId = zooKeeper.getSessionId();
        byte[] sessionPassword = zooKeeper.getSessionPasswd();

        LOGGER.debug("连接状态：{}", zooKeeper.getState());

        // 开始会话重连
        LOGGER.debug("开始会话重连");

        ZooKeeper zkSession = new ZooKeeper(ZK_SERVER_PATH, TIMEOUT, new ZKConnectSessionWatcher(), sessionId,
                sessionPassword);
        LOGGER.debug("重新连接状态：{}", zkSession.getState());
        Thread.sleep(2000);
        LOGGER.debug("重新连接状态：{}", zkSession.getState());
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        LOGGER.debug("接收到通知：{}", watchedEvent);
    }
}
