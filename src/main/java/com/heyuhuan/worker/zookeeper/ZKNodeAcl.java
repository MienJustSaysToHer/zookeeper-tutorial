package com.heyuhuan.worker.zookeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

public class ZKNodeAcl implements Watcher {

    private ZooKeeper zooKeeper = null;

    private static final String ZK_SERVER_PATH = "127.0.0.1:2181";
    private static final Integer TIMEOUT = 5000;

    public ZKNodeAcl() {
    }

    public ZKNodeAcl(String connectString) {
        try {
            zooKeeper = new ZooKeeper(connectString, TIMEOUT, new ZKNodeAcl());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void createZKNode(String path, byte[] data, List<ACL> acls) {
        String result = "";
        try {
            result = zooKeeper.create(path, data, acls, CreateMode.PERSISTENT);
            System.out.println("创建节点：\t" + result + "\t成功...");
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws NoSuchAlgorithmException {
        ZKNodeAcl zkServer = new ZKNodeAcl(ZK_SERVER_PATH);
//        zkServer.createZKNode("/aclimooc2", "test".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE);

//        List<ACL> acls = new ArrayList<>();
//        Id imooc1 = new Id("digest", DigestAuthenticationProvider.generateDigest("imooc1:123456"));
//        Id imooc2 = new Id("digest", DigestAuthenticationProvider.generateDigest("imooc2:123456"));
//        acls.add(new ACL(ZooDefs.Perms.ALL, imooc1));
//        acls.add(new ACL(ZooDefs.Perms.READ, imooc2));
//        acls.add(new ACL(ZooDefs.Perms.DELETE | ZooDefs.Perms.CREATE, imooc2));
//        zkServer.createZKNode("/aclimooc/testdigest2", "testdigest".getBytes(), acls);

        zkServer.zooKeeper.addAuthInfo("digest", "imooc002:123456".getBytes());
        zkServer.createZKNode("/aclimooc/testdigest2/childtest", "childtest".getBytes(),
                ZooDefs.Ids.CREATOR_ALL_ACL);
    }

    @Override
    public void process(WatchedEvent event) {

    }
}
