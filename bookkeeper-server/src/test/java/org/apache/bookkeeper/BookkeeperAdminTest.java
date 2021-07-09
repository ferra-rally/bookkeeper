package org.apache.bookkeeper;

import org.apache.bookkeeper.client.*;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.replication.ReplicationException;
import org.apache.bookkeeper.util.*;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeoutException;

import static org.apache.bookkeeper.util.IOUtils.createTempDir;

@RunWith(value = Parameterized.class)
public class BookkeeperAdminTest {
    private ZooKeeperServerUtil zooKeeperServerUtil;
    private BookKeeper bookKeeper;
    private int ensSize;
    private int wQuorum;
    private int rQuorum;
    private int numOfBookies;
    private LedgerHandle ledgerHandle;
    private BookieServerUtil bookieServerUtil;
    private BookKeeperAdmin admin;

    public BookkeeperAdminTest(int numOfBookies, int ensSize, int wQuorum, int rQuorum) {
        this.numOfBookies = numOfBookies;
        this.ensSize = ensSize;
        this.wQuorum = wQuorum;
        this.rQuorum = rQuorum;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> getTestParameters() {
        return Arrays.asList(new Object[][]{
                {7, 3, 3, 3},
                {3, 3, 3, 3},
                {7, 3, 3, 3},
        });
    }

    @Before
    public void configure() throws BKException, IOException, InterruptedException, KeeperException {
        zooKeeperServerUtil = new ZooKeeperServerUtil(21810);

        bookieServerUtil = new BookieServerUtil(zooKeeperServerUtil);
        bookieServerUtil.startBookies(numOfBookies);
        ClientConfiguration config = new ClientConfiguration();
        config.setAddEntryTimeout(5000);
        bookKeeper = new BookKeeper(config, zooKeeperServerUtil.getZooKeeperClient());

        ledgerHandle = bookKeeper.createLedger(ensSize, wQuorum, rQuorum, BookKeeper.DigestType.CRC32, "A".getBytes(), Collections.emptyMap());
        admin = new BookKeeperAdmin(bookKeeper);
    }

    @After
    public void cleanup() {
        zooKeeperServerUtil.stop();
    }

    @Test
    public void test() {

    }

    @Test
    public void areEntriesOfLedgerSoredInTheBookieTest() throws UnknownHostException {
        int count = 0;
        for (int i = 0; i < bookieServerUtil.getBookieNumer(); i++) {
            BookieServer bookieServer = bookieServerUtil.getBookie(i);
            if (BookKeeperAdmin.areEntriesOfLedgerStoredInTheBookie(ledgerHandle.getId(), bookieServer.getBookieId(), ledgerHandle.getLedgerMetadata()))
                count++;
        }
        Assert.assertEquals(ensSize, count);
        count = 0;

        LedgerManager manager = bookKeeper.getLedgerManager();
        for (int i = 0; i < bookieServerUtil.getBookieNumer(); i++) {
            BookieServer bookieServer = bookieServerUtil.getBookie(i);
            if (BookKeeperAdmin.areEntriesOfLedgerStoredInTheBookie(ledgerHandle.getId(), bookieServer.getBookieId(), manager))
                count++;
        }
        Assert.assertEquals(ensSize, count);
    }

/*
    @Test
    public void asyncRecoverBookieDataTest() throws UnknownHostException, ExecutionException, InterruptedException, org.apache.bookkeeper.client.api.BKException {
        ledgerHandle.append("AAA".getBytes());
        BookieServer bookieServer = bookieServerUtil.getBookie(0);
        bookieServer.shutdown();
        System.out.println("ALIVE: " + bookieServerUtil.numberOfAliveBookies());
        System.out.println("RETURN ENTRY:" + new String(ledgerHandle.readLastEntry().getEntry()));
        final FutureTask<Object> ft = new FutureTask<Object>(() -> {}, new Object());
        RecoverCallback recoverCallback = new RecoverCallback(ft);

        bookieServerUtil.stopBookies(3);

        admin.asyncRecoverBookieData(bookieServer.getBookieId(), recoverCallback, this);
        ft.get();
    }*/

/*
    @Test
    public void asyncOpenLedgerTest() throws org.apache.bookkeeper.client.api.BKException, InterruptedException {

        //final FutureTask<Object> ft = new FutureTask<Object>(() -> {}, new Object());
        //OpenCallback openCallback = new OpenCallback(ft);

        //admin.asyncOpenLedger(ledgerHandle.getId(), openCallback, this);
        //ft.run();

        //LedgerHandle ledgerHandleAsync = openCallback.getLedgerHandle();

        LedgerHandle ledgerHandleAsync = admin.openLedger(ledgerHandle.getId());

        System.out.println("CLOSED: " + ledgerHandleAsync.isClosed());
        ledgerHandleAsync.append("AAA".getBytes());

        admin.readEntries(ledgerHandle.getId(), 0, 0);
    }*/
}
