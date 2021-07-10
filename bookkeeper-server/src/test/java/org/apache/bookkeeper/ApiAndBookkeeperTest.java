package org.apache.bookkeeper;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.WriteHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.util.BookieServerUtil;
import org.apache.bookkeeper.util.PortManager;
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
import java.util.concurrent.ExecutionException;

@RunWith(value = Parameterized.class)
public class ApiAndBookkeeperTest {
    private ZooKeeperServerUtil zooKeeperServerUtil;
    private BookKeeper bookKeeper;
    private byte[] data;
    private int ensSize;
    private int wQuorum;
    private int rQuorum;
    private long id;
    private DigestType digestType;
    private BookieServerUtil bookieServerUtil;

    public ApiAndBookkeeperTest(byte[] data, int ensSize, int wQuorum, int rQuorum, DigestType digestType) {
        this.data = data;
        this.ensSize = ensSize;
        this.wQuorum = wQuorum;
        this.rQuorum = rQuorum;
        this.digestType = digestType;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> getTestParameters() {
        return Arrays.asList(new Object[][]{
                {"data".getBytes(), 3, 2, 2, DigestType.CRC32},
                {"data".getBytes(), 6, 3, 2, DigestType.DUMMY},
                {"data".getBytes(), 6, 3, 2, DigestType.CRC32C},
                {"".getBytes(), 6, 3, 2, DigestType.MAC},
        });
    }

    @Before
    public void configure() throws IOException, InterruptedException, KeeperException, org.apache.bookkeeper.client.api.BKException, ExecutionException {
        zooKeeperServerUtil = new ZooKeeperServerUtil(PortManager.nextFreePort());

        bookieServerUtil = new BookieServerUtil(zooKeeperServerUtil);
        bookieServerUtil.startBookies(ensSize);
        ClientConfiguration config = new ClientConfiguration();
        config.setAddEntryTimeout(5000);
        bookKeeper = new BookKeeper(config, zooKeeperServerUtil.getZooKeeperClient());

        WriteHandle wh = bookKeeper.newCreateLedgerOp()
                .withDigestType(DigestType.CRC32)
                .withPassword("A".getBytes())
                .withEnsembleSize(ensSize)
                .withWriteQuorumSize(wQuorum)
                .withAckQuorumSize(rQuorum)
                .execute()
                .get();

        System.out.println("GOT WRITE HANDLE");
        wh.append(data);
        id = wh.getId();
        System.out.println("WROTE ON LEDGER");
    }

    @After
    public void cleanup() {
        bookieServerUtil.stop();
    }

    @Test
    public void apiAndBookkeeper() throws IOException, InterruptedException, KeeperException, BKException {
        System.out.println("OPENING LEDGER");
        LedgerHandle handle = bookKeeper.openLedger(id, BookKeeper.DigestType.fromApiDigestType(digestType), "A".getBytes());
        System.out.println("LEDGER OPENED");

        Assert.assertArrayEquals(data, handle.readLastEntry().getEntry());
    }
}
