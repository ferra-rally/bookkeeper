package org.apache.bookkeeper;

import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.client.*;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.replication.ReplicationException;
import org.apache.bookkeeper.tls.SecurityException;
import org.apache.bookkeeper.util.AddCallback;
import org.apache.bookkeeper.util.BookieServerUtil;
import org.apache.bookkeeper.util.ReadCallback;
import org.apache.bookkeeper.util.ZooKeeperServerUtil;
import org.apache.zookeeper.KeeperException;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

@RunWith(value = Parameterized.class)
public class LedgerHandleTest {
    private ZooKeeperServerUtil zooKeeperServerUtil;
    private BookKeeper bookKeeper;
    private LedgerHandle ledgerHandle;
    private byte[] data;
    private int firstEntry;
    private int lastEntry;
    private int ensSize;
    private int wQuorum;
    private int rQuorum;
    private int stopBookies;
    private BookKeeper.DigestType digestType;
    private BookieServerUtil bookieServerUtil;

    /* Digests
    CRC32
    CRC32C
    DUMMY
    MAC
     */
    @Parameterized.Parameters
    public static Collection<Object[]> getTestParameters() {
        return Arrays.asList(new Object[][]{
                {"Test0".getBytes(), BookKeeper.DigestType.MAC, 1, 1, 3, 2, 2, 1}
        });
    }

    public LedgerHandleTest(byte[] data, BookKeeper.DigestType digestTypeParam, int firstEntry, int lastEntry, int ensSize, int wQuorum, int rQuorum, int stopBookies) {
        this.data = data;
        this.digestType = digestTypeParam;
        this.firstEntry = firstEntry;
        this.lastEntry = lastEntry;
        this.ensSize = ensSize;
        this.wQuorum = wQuorum;
        this.rQuorum = rQuorum;
        this.stopBookies = stopBookies;
    }

    @Before
    public void config() throws IOException, InterruptedException, KeeperException, BKException {
        zooKeeperServerUtil = new ZooKeeperServerUtil(21810);

        bookieServerUtil = new BookieServerUtil(zooKeeperServerUtil);
        bookieServerUtil.startBookies(ensSize);
        ClientConfiguration config = new ClientConfiguration();
        config.setAddEntryTimeout(5000);
        bookKeeper = new BookKeeper(config, zooKeeperServerUtil.getZooKeeperClient());

        ledgerHandle = bookKeeper.createLedger(ensSize,wQuorum, rQuorum, digestType, "A".getBytes(), Collections.emptyMap());
    }

    @After
    public void cleanup() {
        zooKeeperServerUtil.stop();
    }

    @Test
    public void addSingleEntryTest() throws BKException, InterruptedException {
        ledgerHandle.addEntry(data);

        LedgerEntry fetchedEntry = ledgerHandle.readLastEntry();
        byte[] fetched = fetchedEntry.getEntry();

        Assert.assertArrayEquals(data, fetched);
    }

    @Test
    public void appendTest() throws org.apache.bookkeeper.client.api.BKException, InterruptedException {
        ledgerHandle.append(data);

        LedgerEntry fetchedEntry = ledgerHandle.readLastEntry();
        byte[] fetched = fetchedEntry.getEntry();

        Assert.assertArrayEquals(data, fetched);
    }

    @Test
    public void addSingleEntryAsyncTest() throws BKException, InterruptedException, ExecutionException {

        //Use future task to see when callback is called
        final FutureTask<Object> ft = new FutureTask<Object>(() -> {}, new Object());
        AddCallback addCallback = new AddCallback(ft);
        ledgerHandle.asyncAddEntry(data, addCallback, this);

        ft.get();
        LedgerEntry fetchedEntry = ledgerHandle.readLastEntry();
        byte[] fetched = fetchedEntry.getEntry();

        Assert.assertArrayEquals(data, fetched);
    }

    @Test
    public void addSingleEntryStopBookiesTest() throws BKException, InterruptedException {
        ledgerHandle.addEntry(data);

        //TODO edit
        bookieServerUtil.stopBookies(stopBookies);

        LedgerEntry fetchedEntry = ledgerHandle.readLastEntry();
        byte[] fetched = fetchedEntry.getEntry();

        Assert.assertArrayEquals(data, fetched);
    }

    private static byte[] convertIntToArray(int i) {
        ByteBuffer b = ByteBuffer.allocate(4);
        b.putInt(i);

        return b.array();
    }
}
