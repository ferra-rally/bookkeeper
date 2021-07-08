package org.apache.bookkeeper;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.util.BookieServerUtil;
import org.apache.bookkeeper.util.PortManager;
import org.apache.bookkeeper.util.ReadCallback;
import org.apache.bookkeeper.util.ZooKeeperServerUtil;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

@RunWith(value = Parameterized.class)
public class LedgerEntryReadEntries {
    private ZooKeeperServerUtil zooKeeperServerUtil;
    private BookKeeper bookKeeper;
    private LedgerHandle ledgerHandle;
    private byte[] data;
    private int firstEntry;
    private int lastEntry;
    private int ensSize;
    private int wQuorum;
    private int rQuorum;
    private BookKeeper.DigestType digestType;
    private int stopBookies;
    private BookieServerUtil bookieServerUtil;

    @Parameterized.Parameters
    public static Collection<Object[]> getTestParameters() {
        return Arrays.asList(new Object[][]{
                {"Test0".getBytes(), BookKeeper.DigestType.MAC, 10, 20, 3, 3, 1, 1},
                {"Test0".getBytes(), BookKeeper.DigestType.MAC, 20, 10, 3, 3, 3, 1},
                {"Test0".getBytes(), BookKeeper.DigestType.MAC, 1, 10, 6, 6, 3, 2}
        });
    }

    public LedgerEntryReadEntries(byte[] data, BookKeeper.DigestType digestTypeParam, int firstEntry, int lastEntry, int ensSize, int wQuorum, int rQuorum, int stopBookies) {
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
    public void configure() throws IOException, InterruptedException, KeeperException, BKException {
        zooKeeperServerUtil = new ZooKeeperServerUtil(21810);

        bookieServerUtil = new BookieServerUtil(zooKeeperServerUtil);
        bookieServerUtil.startBookies(ensSize);
        ClientConfiguration config = new ClientConfiguration();
        config.setAddEntryTimeout(5000);
        bookKeeper = new BookKeeper(config, zooKeeperServerUtil.getZooKeeperClient());

        ledgerHandle = bookKeeper.createLedger(ensSize,wQuorum, rQuorum, digestType, "A".getBytes(), Collections.emptyMap());

        for(int i = 0; i < firstEntry + lastEntry; i++) {
            if((i >= firstEntry) && (i <= lastEntry)) {
                ledgerHandle.addEntry(data);
            } else {
                ledgerHandle.addEntry(("test" + i).getBytes());
            }
            System.out.println("AAAAAA: " + new String(ledgerHandle.readLastEntry().getEntry()));
        }
    }

    @After
    public void cleanup() {
        zooKeeperServerUtil.stop();
    }

    @Test
    public void readEntryWithOffset() throws InterruptedException {
        try {
            Enumeration<LedgerEntry> fetchedEntries = ledgerHandle.readEntries(firstEntry, lastEntry);
            while (fetchedEntries.hasMoreElements()) {
                byte[] bytes = fetchedEntries.nextElement().getEntry();
                Assert.assertArrayEquals(data, bytes);
            }
        } catch (BKException e) {
            if(lastEntry < firstEntry) {
                Assert.assertTrue(true);
            } else Assert.fail();
        }
    }

    @Test
    public void asyncReadEntriesTest() throws ExecutionException, InterruptedException, org.apache.bookkeeper.client.api.BKException {
        final FutureTask<Object> ft = new FutureTask<Object>(() -> {}, new Object());
        ReadCallback readCallback = new ReadCallback(ft);
        ledgerHandle.asyncReadEntries(firstEntry, lastEntry, readCallback, this);

        ft.get();
        Enumeration<LedgerEntry> entryEnumeration = readCallback.getEntryEnumeration();

        if(entryEnumeration == null) {
            if(lastEntry < firstEntry) {
                Assert.assertTrue(true);
            } else Assert.fail();
        } else {
            while (entryEnumeration.hasMoreElements()) {
                byte[] bytes = entryEnumeration.nextElement().getEntry();
                Assert.assertArrayEquals(data, bytes);
            }
        }
    }

    @Test
    public void addSingleEntryStopBookiesTest() throws BKException, InterruptedException {
        ledgerHandle.addEntry(data);

        bookieServerUtil.stopBookies(stopBookies);

        LedgerEntry fetchedEntry = ledgerHandle.readLastEntry();
        byte[] fetched = fetchedEntry.getEntry();

        Assert.assertArrayEquals(data, fetched);
    }
}
