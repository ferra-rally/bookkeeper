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
import java.util.Enumeration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

@RunWith(value = Parameterized.class)
public class LedgerHandleTest {
    private ZooKeeperServerUtil zooKeeperServerUtil;
    private BookKeeper bookKeeper;
    private LedgerHandle ledgerHandle;
    private byte[] data;
    private int offset;
    private int firstEntry;
    private int lastEntry;
    private BookKeeper.DigestType digestType;

    /* Digests
    CRC32
    CRC32C
    DUMMY
    MAC
     */
    @Parameterized.Parameters
    public static Collection<Object[]> getTestParameters() {
        return Arrays.asList(new Object[][]{
                {"Test0".getBytes(), BookKeeper.DigestType.MAC, 1, 1, 1},
                {"Test0".getBytes(), BookKeeper.DigestType.CRC32, 2, 1, 2},
                {convertIntToArray(1), BookKeeper.DigestType.CRC32, 3, 1, 1},
                {convertIntToArray(1), BookKeeper.DigestType.CRC32C, 3, 3, 1},
                {convertIntToArray(1), BookKeeper.DigestType.DUMMY, 3, 1, 1}
        });
    }

    public LedgerHandleTest(byte[] data, BookKeeper.DigestType digestTypeParam, int offset, int firstEntry, int lastEntry) {
        this.data = data;
        this.digestType = digestTypeParam;
        this.offset = offset;
        this.firstEntry = firstEntry;
        this.lastEntry = lastEntry;
    }

    @Before
    public void config() throws IOException, InterruptedException, KeeperException, BKException, ReplicationException.CompatibilityException, ReplicationException.UnavailableException, SecurityException, BookieException {
        zooKeeperServerUtil = new ZooKeeperServerUtil(21810);

        BookieServerUtil bookieServerUtil = new BookieServerUtil();
        bookieServerUtil.startBookies(3, zooKeeperServerUtil.getZooKeeperAddress());
        ClientConfiguration config = new ClientConfiguration();
        config.setAddEntryTimeout(5000);
        bookKeeper = new BookKeeper(config, zooKeeperServerUtil.getZooKeeperClient());

        ledgerHandle = bookKeeper.createLedger(digestType, "A".getBytes());
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

    /*
    @Test
    public void addSingleEntryWithOffsetTest() throws BKException, InterruptedException {
        for(int i = 0; i < offset; i++) {
            ledgerHandle.addEntry("Test".getBytes());
        }
        ledgerHandle.addEntry(data,1, data.length);

        LedgerEntry fetchedEntry = ledgerHandle.readLastEntry();
        byte[] fetched = fetchedEntry.getEntry();

        Assert.assertArrayEquals(data, fetched);
    }*/

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
    public void asyncReadEntriesTest() throws ExecutionException, InterruptedException {

        final FutureTask<Object> ft = new FutureTask<Object>(() -> {}, new Object());
        ReadCallback readCallback = new ReadCallback(ft);
        ledgerHandle.asyncReadEntries(1, 1, readCallback, this);

        ft.get();
        Enumeration<LedgerEntry> entryEnumeration = readCallback.getEntryEnumeration();

        Assert.assertNotNull(entryEnumeration);
    }

    private static byte[] convertIntToArray(int i) {
        ByteBuffer b = ByteBuffer.allocate(4);
        b.putInt(i);

        return b.array();
    }
}
