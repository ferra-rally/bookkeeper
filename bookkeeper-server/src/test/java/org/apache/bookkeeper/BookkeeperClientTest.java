package org.apache.bookkeeper;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;

import org.apache.bookkeeper.util.ZooKeeperServerUtil;
import org.apache.zookeeper.*;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Assert;

import java.io.IOException;

public class BookkeeperClientTest {
    private static ZooKeeperServerUtil zooKeeperServerUtil;
    protected static ZooKeeper zkc;

    public BookkeeperClientTest() {
    }

    @BeforeClass
    public static void configure() throws IOException, InterruptedException, KeeperException {
        //Start zookkeeper service
        zooKeeperServerUtil = new ZooKeeperServerUtil(21810);
    }

    @Test
    public void bookkeeperCreationTest() throws InterruptedException {

        try {
            BookKeeper bkc = new BookKeeper("127.0.0.1:21810");
            Assert.assertNotNull(bkc);
        } catch (BKException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
