package com.heyuhuan.worker.zookeeper;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class ZKGetChildrenList implements Watcher {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZKGetNodeData.class);

    private ZooKeeper zooKeeper = null;

    private static final String ZK_SERVER_PATH = "127.0.0.1:2181";
    private static final Integer TIMEOUT = 5000;

    private static final CountDownLatch countDown = new CountDownLatch(1);

    public ZKGetChildrenList() {
    }

    public ZKGetChildrenList(String connectString) {
        try {
            zooKeeper = new ZooKeeper(connectString, TIMEOUT, new ZKGetChildrenList());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws KeeperException, InterruptedException {
        ZKGetChildrenList zkServer = new ZKGetChildrenList(ZK_SERVER_PATH);

//        List<String> strChildList = zkServer.zooKeeper.getChildren("/imooc", true);
//        for (String s : strChildList) {
//            LOGGER.debug(s);
//        }

        // 异步调用
        String ctx = "{'callback':'ChildrenCallback'}";
        zkServer.zooKeeper.getChildren("/imooc", true, new AsyncCallback.ChildrenCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, List<String> children) {
                for (String s : children) {
                    LOGGER.debug(s);
                }
                LOGGER.debug("ChildrenCallback:" + path);
                LOGGER.debug((String) ctx);
            }
        }, ctx);

        countDown.await();
    }

    @Override
    public void process(WatchedEvent event) {
        try {
            if (event.getType() == Event.EventType.NodeChildrenChanged) {
                ZKGetChildrenList zkServer = new ZKGetChildrenList(ZK_SERVER_PATH);
                List<String> strChildList = zkServer.zooKeeper.getChildren(event.getPath(), false);
                for (String s : strChildList) {
                    LOGGER.debug(s);
                }
                countDown.countDown();
            } else if (event.getType() == Event.EventType.NodeCreated) {

            } else if (event.getType() == Event.EventType.NodeDataChanged) {

            } else if (event.getType() == Event.EventType.NodeDeleted) {

            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
