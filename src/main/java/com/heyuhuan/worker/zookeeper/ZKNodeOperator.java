package com.heyuhuan.worker.zookeeper;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class ZKNodeOperator implements Watcher {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZKConnectSessionWatcher.class);

    private ZooKeeper zooKeeper = null;

    private static final String ZK_SERVER_PATH = "127.0.0.1:2181";
    private static final Integer TIMEOUT = 5000;

    public ZKNodeOperator() {
    }

    public ZKNodeOperator(String connectString) {
        try {
            zooKeeper = new ZooKeeper(connectString, TIMEOUT, new ZKNodeOperator());
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

    public void createZKNode(String path, byte[] data, List<ACL> acls) {
        String ctx = "{'create':'success'}";
        zooKeeper.create(path, data, acls, CreateMode.PERSISTENT, new CreateCallback(), ctx);
        LOGGER.debug("创建节点成功...");
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void process(WatchedEvent event) {

    }

    public static void main(String[] args) throws KeeperException, InterruptedException {
        ZKNodeOperator zkNodeOperator = new ZKNodeOperator(ZK_SERVER_PATH);
//        zkNodeOperator.createZKNode("/testnode", "testnode".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE);
        Stat stat = new Stat();
        byte[] data = zkNodeOperator.zooKeeper.getData("/testnode", false, stat);
        LOGGER.debug(new String(data));
        LOGGER.debug(String.valueOf(stat));
        stat = zkNodeOperator.zooKeeper.setData("/testnode", "xyz".getBytes(), stat.getVersion());
        LOGGER.debug(String.valueOf(stat.getVersion()));
    }
}
