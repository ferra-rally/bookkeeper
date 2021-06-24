package org.apache.bookkeeper.util;

import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.apache.zookeeper.*;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.test.QuorumUtil;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

import static org.apache.bookkeeper.util.BookKeeperConstants.AVAILABLE_NODE;
import static org.apache.bookkeeper.util.BookKeeperConstants.READONLY;

public class ZooKeeperServerUtil {
    private String loopback;
    private int port;
    private String address;

    private ZooKeeperServer zooKeeperServer;
    private ZooKeeper zooKeeperClient;
    private NIOServerCnxnFactory serverFactory;

    public ZooKeeperServerUtil(int port) throws IOException, InterruptedException, KeeperException {
        //Create temporary zookeeper directory
        File temp = IOUtils.createTempDir("zookeeper", "temp");

        String loopback = InetAddress.getLoopbackAddress().getHostAddress();
        InetSocketAddress zkaddr = new InetSocketAddress(loopback, port);
        address = loopback + ":" + port;

        serverFactory = new NIOServerCnxnFactory();

        zooKeeperServer = new ZooKeeperServer(temp, temp, ZooKeeperServer.DEFAULT_TICK_TIME);

        serverFactory.configure(zkaddr, 1000);
        serverFactory.startup(zooKeeperServer);

        zooKeeperClient = ZooKeeperClient.newBuilder()
                .connectString(address)
                .sessionTimeoutMs(10000)
                .build();

        Transaction txn = zooKeeperClient.transaction();
        txn.create("/ledgers", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        txn.create("/ledgers" + "/" + AVAILABLE_NODE, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        txn.create("/ledgers" + "/" + AVAILABLE_NODE + "/" + READONLY, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
        txn.commit();
    }

    public ZooKeeper getZooKeeperClient() { return zooKeeperClient;}

    public String getZooKeeperAddress() { return address;}

    public ZooKeeperServer getZooKeeperServer() { return zooKeeperServer;}

    public void stop() {
        serverFactory.shutdown();
    }
}
