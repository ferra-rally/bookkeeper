package org.apache.bookkeeper;

import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.replication.ReplicationException;
import org.apache.bookkeeper.tls.SecurityException;
import org.apache.bookkeeper.util.BookieServerUtil;
import org.apache.bookkeeper.util.ZooKeeperServerUtil;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;

@RunWith(value = Parameterized.class)
public class LedgerHandleTest {
    private ZooKeeperServerUtil zooKeeperServerUtil;
    private BookKeeper bookKeeper;
    private LedgerHandle ledgerHandle;
    private byte[] data;
    private BookKeeper.DigestType digestType;

    @Parameterized.Parameters
    public static Collection<Object[]> getTestParameters() {
        return Arrays.asList(new Object[][]{
                {"Test0".getBytes(), BookKeeper.DigestType.MAC},
                {"Test0".getBytes(), BookKeeper.DigestType.CRC32},
                {convertIntToArray(1), BookKeeper.DigestType.CRC32}
        });
    }

    public LedgerHandleTest(byte[] data, BookKeeper.DigestType digestType) {
        this.data = data;
        this.digestType = digestType;
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
        ByteBuffer entry = ByteBuffer.allocate(data.length);
        entry.put(data);

        ledgerHandle.addEntry(entry.array());

        LedgerEntry fetchedEntry = ledgerHandle.readLastEntry();

        ByteBuffer result = ByteBuffer.wrap(fetchedEntry.getEntry());
        byte[] fetched = result.array();

        Assert.assertArrayEquals(data, fetched);
    }

    private static byte[] convertIntToArray(int i) {
        ByteBuffer b = ByteBuffer.allocate(4);
        b.putInt(i);

        return b.array();
    }
}
