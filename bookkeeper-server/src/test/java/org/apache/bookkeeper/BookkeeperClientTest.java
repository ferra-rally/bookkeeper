package org.apache.bookkeeper;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;

import org.apache.bookkeeper.util.ZooKeeperServerUtil;
import org.apache.zookeeper.*;
import org.junit.*;

import java.io.IOException;

public class BookkeeperClientTest {
    private ZooKeeperServerUtil zooKeeperServerUtil;
    protected ZooKeeper zkc;

    public BookkeeperClientTest() {
    }

    @Before
    public void configure() throws IOException, InterruptedException, KeeperException {
        //Start zookkeeper service
        zooKeeperServerUtil = new ZooKeeperServerUtil(21810);
    }

    @After
    public void cleanup() {
        zooKeeperServerUtil.stop();
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
