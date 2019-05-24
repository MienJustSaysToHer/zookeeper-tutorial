package com.heyuhuan.worker.zookeeper;

import org.apache.zookeeper.AsyncCallback;

public class CreateCallback implements AsyncCallback.StringCallback {

    @Override
    public void processResult(int rc, String path, Object ctx, String name) {
        System.out.println("创建节点：" + path);
        System.out.println((String) ctx);
        System.out.println(name);
    }
}
