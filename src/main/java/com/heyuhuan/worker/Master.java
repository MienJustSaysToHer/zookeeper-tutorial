package com.heyuhuan.worker;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

public class Master implements Watcher {

    ZooKeeper zk;

    String hostPort;

    Master(String hostPort) {
        this.hostPort = hostPort;
    }

    void startZK() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    @Override
    public void process(WatchedEvent e) {
        System.out.println(e);
    }

    public static void main(String[] args) throws Exception {
        Master m = new Master(args[0]);
        m.startZK();
        Thread.sleep(60000);
    }
}
