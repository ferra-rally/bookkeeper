package org.apache.bookkeeper;

import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.replication.ReplicationException;
import org.apache.bookkeeper.tls.SecurityException;
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

@RunWith(value = Parameterized.class)
public class LedgerHandleTest {
    private ZooKeeperServerUtil zooKeeperServerUtil;
    private BookKeeper bookKeeper;
    private LedgerHandle ledgerHandle;
    private byte[] data;

    @Parameterized.Parameters
    public static Collection<Object[]> getTestParameters() {
        return Arrays.asList(new Object[][]{
                {"Test0".getBytes()},
                {"Test1".getBytes()}
        });
    }

    public LedgerHandleTest(byte[] data) {
        this.data = data;
    }

    @Before
    public void config() throws IOException, InterruptedException, KeeperException, BKException, ReplicationException.CompatibilityException, ReplicationException.UnavailableException, SecurityException, BookieException {
        zooKeeperServerUtil = new ZooKeeperServerUtil(21810);

        BookieServerUtil bookieServerUtil = new BookieServerUtil();
        bookieServerUtil.startBookies(3, zooKeeperServerUtil.getZooKeeperAddress());
        ClientConfiguration config = new ClientConfiguration();
        config.setAddEntryTimeout(5000);
        bookKeeper = new BookKeeper(config, zooKeeperServerUtil.getZooKeeperClient());

        ledgerHandle = bookKeeper.createLedger(BookKeeper.DigestType.MAC, "A".getBytes());
    }

    @After
    public void cleanup() {
        zooKeeperServerUtil.stop();
    }

    @Test
    public void addEntryTest() throws BKException, InterruptedException {
        ledgerHandle.addEntry(data);

        LedgerEntry entry = ledgerHandle.readLastEntry();
        byte[] fetched = entry.getEntry();

        Assert.assertArrayEquals(fetched, data);
    }
}
