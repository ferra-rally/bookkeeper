package org.apache.bookkeeper;

import com.google.common.net.InetAddresses;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.zk.ZKMetadataDriverBase;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.replication.ReplicationException;
import org.apache.bookkeeper.tls.SecurityException;
import org.apache.bookkeeper.util.BookKeeperConstants;
import org.apache.bookkeeper.util.BookieServerUtil;
import org.apache.bookkeeper.util.PortManager;
import org.apache.bookkeeper.util.ZooKeeperServerUtil;
import org.apache.zookeeper.*;
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

import static org.apache.bookkeeper.util.BookKeeperConstants.AVAILABLE_NODE;
import static org.apache.bookkeeper.util.BookKeeperConstants.READONLY;
import static org.apache.bookkeeper.util.IOUtils.createTempDir;

@RunWith(value = Parameterized.class)
public class BookkeeperAdminInitNewClusterTest {
    private final ServerConfiguration baseConf = TestBKConfiguration.newServerConfiguration();
    private ZooKeeper zkc;
    private ZooKeeperServerUtil zooKeeperServerUtil;
    private BookieServerUtil bookieServerUtil;
    private List<BookieServer> bookies = new ArrayList<>();
    private String ledgerRoot;
    private int numBookies;
    private BookKeeper bookKeeper;
    private BookKeeperAdmin admin;

    public BookkeeperAdminInitNewClusterTest(String ledgerRoot, int numBookies) {
        this.ledgerRoot = ledgerRoot;
        this.numBookies = numBookies;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> getTestParameters() {
        return Arrays.asList(new Object[][]{
                {"ledgertest", 3},
                {"", 3},
                {null, 3},
                {"ledgertest", 6},
        });
    }

    @Before
    public void configure() throws IOException, InterruptedException, KeeperException, BKException {
        zooKeeperServerUtil = new ZooKeeperServerUtil(PortManager.nextFreePort());

        zkc = zooKeeperServerUtil.getZooKeeperClient();
        bookieServerUtil = new BookieServerUtil(zooKeeperServerUtil);
        bookieServerUtil.startBookies(numBookies);
        ClientConfiguration config = new ClientConfiguration();
        config.setAddEntryTimeout(5000);
        bookKeeper = new BookKeeper(config, zooKeeperServerUtil.getZooKeeperClient());
        admin = new BookKeeperAdmin(bookKeeper);
    }

    @After
    public void cleanup() {
        zooKeeperServerUtil.stop();
    }

    @Test
    public void initNewClusterTest() throws Exception {
        String ledgerRootPath = "/" + ledgerRoot;
        baseConf.setMetadataServiceUri("zk://127.0.0.1:" + zooKeeperServerUtil.getPort() + ledgerRootPath);
        ServerConfiguration config = new ServerConfiguration(baseConf);

        Transaction txn = zkc.transaction();
        txn.create(ledgerRootPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        txn.create(ledgerRootPath + "/" + AVAILABLE_NODE, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        txn.create(ledgerRootPath + "/" + AVAILABLE_NODE + "/" + READONLY, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);

        //Check if the cluster is initialized
        try {
            Assert.assertTrue(BookKeeperAdmin.initNewCluster(config));
        } catch (IllegalArgumentException e) {
            if (!ledgerRootPath.equals("/")) {
                Assert.fail();
            } else {
                return;
            }
        }


        for(int i = 0; i < numBookies; i++) {
            ServerConfiguration conf = new ServerConfiguration(baseConf);
            conf.setBookieId("TESTBOOKIE" + i);

            try {
                File journalDir = createTempDir("bookie", "journal" + i);
                File ledgerDirNames = createTempDir("bookie", "ledger" + i);

                conf.setJournalDirName(journalDir.getPath());
                conf.setLedgerDirNames(new String[]{ledgerDirNames.getPath()});
                int port = PortManager.nextFreePort();

                conf.setBookiePort(port);

                BookieServer server = new BookieServer(conf);
                bookies.add(server);

                server.start();
            } catch (Exception e) {
                e.printStackTrace();
                Assert.fail();
            }
        }

        //Test Ledger creation
        LedgerHandle ledgerHandle = bookKeeper.createLedger(numBookies, numBookies, numBookies, BookKeeper.DigestType.CRC32, "A".getBytes(), Collections.emptyMap());
        Assert.assertNotNull(ledgerHandle);
    }
}
