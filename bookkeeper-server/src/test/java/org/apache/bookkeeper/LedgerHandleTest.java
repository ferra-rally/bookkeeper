package org.apache.bookkeeper;

import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.client.*;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.replication.ReplicationException;
import org.apache.bookkeeper.tls.SecurityException;
import org.apache.bookkeeper.util.*;
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
                {"Test0".getBytes(), BookKeeper.DigestType.CRC32, 3, 3, 3, 0},
                {"".getBytes(), BookKeeper.DigestType.CRC32, 3, 2, 1, 1},
                {"test".getBytes(), BookKeeper.DigestType.DUMMY, 6, 3, 2, 1},
                {new byte[] {0x00, 0x08}, BookKeeper.DigestType.MAC, 6, 3, 3, 3},
        });
    }

    public LedgerHandleTest(byte[] data, BookKeeper.DigestType digestTypeParam, int ensSize, int wQuorum, int rQuorum, int stopBookies) {
        this.data = data;
        this.digestType = digestTypeParam;
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
        bookieServerUtil.stopBookies(stopBookies);
        ledgerHandle.addEntry(data);

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
