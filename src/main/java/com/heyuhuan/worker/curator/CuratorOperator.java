package com.heyuhuan.worker.curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;

import java.util.List;

public class CuratorOperator {

    public CuratorFramework client = null;
    public static final String ZK_SERVER_PATH = "127.0.0.1:2181";

    public CuratorOperator() {
//        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 5);

        RetryPolicy retryPolicy = new RetryNTimes(3, 5000);

        client = CuratorFrameworkFactory.builder()
                .connectString(ZK_SERVER_PATH)
                .sessionTimeoutMs(10000).retryPolicy(retryPolicy)
                .namespace("workspace").build();
        client.start();
    }

    public void closeZKClient() {
        if (client != null) {
            this.client.close();
        }
    }

    public static void main(String[] args) throws Exception {
        CuratorOperator cto = new CuratorOperator();
        boolean isZkCuratorStarted = cto.client.isStarted();
        System.out.println("当前客户的状态：" + (isZkCuratorStarted ? "连接中" : "已关闭"));

        String nodePath = "/super/imooc";
//        byte[] data = "supreme".getBytes();
//        cto.client.create().creatingParentsIfNeeded()
//                .withMode(CreateMode.PERSISTENT)
//                .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
//                .forPath(nodePath, data);

//        byte[] newData = "batman".getBytes();
//        cto.client.setData()
//                .withVersion(0)
//                .forPath(nodePath, newData);

//        cto.client.delete()
//                .guaranteed()
//                .deletingChildrenIfNeeded()
//                .withVersion(1)
//                .forPath(nodePath);

//        Stat stat = new Stat();
//        byte[] data2 = cto.client.getData().storingStatIn(stat).forPath(nodePath);
//        System.out.println("节点" + nodePath + "的数据为：" + new String(data2));
//        System.out.println("该节点的版本号为：" + stat.getVersion());
//
//        List<String> children = cto.client.getChildren().forPath(nodePath);
//        for (String s : children) {
//            System.out.println(s);
//        }
//
//        Stat statExist = cto.client.checkExists().forPath(nodePath + "/abc");
//        System.out.println(statExist);

//        cto.client.getData().usingWatcher(new MyCuratorWatcher()).forPath(nodePath);
//        final NodeCache nodeCache = new NodeCache(cto.client, nodePath);
//        nodeCache.start(true);
//        if (nodeCache.getCurrentData() != null) {
//            System.out.println("节点初始化数据为：" + new String(nodeCache.getCurrentData().getData()));
//        } else {
//            System.out.println("节点初始化数据为空...");
//        }
//        nodeCache.getListenable().addListener(new NodeCacheListener() {
//            @Override
//            public void nodeChanged() throws Exception {
//                String data = new String(nodeCache.getCurrentData().getData());
//                System.out.println("节点路径：" + nodeCache.getCurrentData().getPath() + "数据：" + data);
//            }
//        });

        PathChildrenCache pathChildrenCache = new PathChildrenCache(cto.client, nodePath, true);
        pathChildrenCache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
        List<ChildData> childDataList = pathChildrenCache.getCurrentData();
        for (ChildData childData : childDataList) {
            String childDataString = new String(childData.getData());
            System.out.println(childDataString);
        }
        pathChildrenCache.getListenable().addListener(new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                if (PathChildrenCacheEvent.Type.INITIALIZED.equals(event.getType())) {
                    System.out.println("子节点初始化ok...");
                } else if (PathChildrenCacheEvent.Type.CHILD_ADDED.equals(event.getType())) {
                    System.out.println("子节点添加..." + new String(event.getData().getData()));
                }
            }
        });

        Thread.sleep(30000000);

        cto.closeZKClient();
        isZkCuratorStarted = cto.client.isStarted();
        System.out.println("当前客户的状态：" + (isZkCuratorStarted ? "连接中" : "已关闭"));
    }
}
