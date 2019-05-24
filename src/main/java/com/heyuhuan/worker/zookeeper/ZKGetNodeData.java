package com.heyuhuan.worker.zookeeper;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class ZKGetNodeData implements Watcher {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZKGetNodeData.class);

    private ZooKeeper zooKeeper = null;

    private static final String ZK_SERVER_PATH = "127.0.0.1:2181";
    private static final Integer TIMEOUT = 5000;
    private static Stat stat = new Stat();

    public ZKGetNodeData() {
    }

    public ZKGetNodeData(String connectString) {
        try {
            zooKeeper = new ZooKeeper(connectString, TIMEOUT, new ZKGetNodeData());
        } catch (IOException e) {
            e.printStackTrace();
            if (zooKeeper != null) {
                try {
                    zooKeeper.close();
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            }
        }
    }

    private static CountDownLatch countDown = new CountDownLatch(1);

    public static void main(String[] args) throws KeeperException, InterruptedException {
        ZKGetNodeData zkServer = new ZKGetNodeData(ZK_SERVER_PATH);

        byte[] data = zkServer.zooKeeper.getData("/imooc", true, stat);
        String result = new String(data);
        LOGGER.debug(result);
        countDown.await();
    }

    @Override
    public void process(WatchedEvent event) {
        try {
            if (event.getType() == Event.EventType.NodeDataChanged) {
                ZKGetNodeData zkServer = new ZKGetNodeData(ZK_SERVER_PATH);
                byte[] resByte = zkServer.zooKeeper.getData("/imooc", false, stat);
                String result = new String(resByte);
                LOGGER.debug(result);
                LOGGER.debug(String.valueOf(stat.getVersion()));
                countDown.countDown();
            } else if (event.getType() == Event.EventType.NodeCreated) {

            } else if (event.getType() == Event.EventType.NodeChildrenChanged) {

            } else if (event.getType() == Event.EventType.NodeDeleted) {

            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
