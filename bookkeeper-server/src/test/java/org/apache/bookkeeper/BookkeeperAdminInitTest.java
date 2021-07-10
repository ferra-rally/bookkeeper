package org.apache.bookkeeper;

import org.apache.bookkeeper.TestBKConfiguration;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookieServer;
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

import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.apache.bookkeeper.util.IOUtils.createTempDir;

@RunWith(value = Parameterized.class)
public class BookkeeperAdminInitTest {
    private ZooKeeperServerUtil zooKeeperServerUtil;
    private BookKeeper bookKeeper;
    private int ensSize = 3;
    private int wQuorum = 2;
    private int rQuorum = 2;
    private LedgerHandle ledgerHandle;
    private BookieServerUtil bookieServerUtil;
    private List<File> ledgers = new ArrayList<>();
    private List<File> jornals = new ArrayList<>();
    private boolean expected;
    private BookKeeperAdmin admin;
    private String journalPath;
    private String ledgerPath;
    private String ledgerBaseRoot;
    private String name;


    @Parameterized.Parameters
    public static Collection<Object[]> getTestParameters() {
        return Arrays.asList(new Object[][]{
                {"TEST", "ledgers", "journaltestpath", "ledgertestpath", true},
                {"TEST", "ledgers", "", "ledger path", true},
                {"TEST", "ledgers", "journalpath", "", true},
                {"BOOKIE0", "ledgers", "journalpath", null, false},
                {"TEST", "ledgers", null, null, true},
                {"TEST", "ledgerswrong", "journaltestpath", "ledgertestpath", true},
        });
    }

    public BookkeeperAdminInitTest(String name, String ledgerBaseRoot, String journalPath, String ledgerPath, boolean expected) {
        this.name = name;
        this.journalPath = journalPath;
        this.ledgerBaseRoot = ledgerBaseRoot;
        this.ledgerPath = ledgerPath;
        this.expected = expected;
    }

    @Before
    public void configure() throws BKException, IOException, InterruptedException, KeeperException {
        zooKeeperServerUtil = new ZooKeeperServerUtil(PortManager.nextFreePort());

        bookieServerUtil = new BookieServerUtil(zooKeeperServerUtil);
        bookieServerUtil.startBookies(ensSize);
        ClientConfiguration config = new ClientConfiguration();
        config.setAddEntryTimeout(5000);
        bookKeeper = new BookKeeper(config, zooKeeperServerUtil.getZooKeeperClient());

        ledgerHandle = bookKeeper.createLedger(ensSize, wQuorum, rQuorum, BookKeeper.DigestType.CRC32, "A".getBytes(), Collections.emptyMap());
        admin = new BookKeeperAdmin(bookKeeper);
        jornals = bookieServerUtil.getJournalDirArray();
        ledgers = bookieServerUtil.getLedgerDirArray();
    }

    @After
    public void cleanup() {
        zooKeeperServerUtil.stop();
    }


    @Test
    public void initBookieTest() throws Exception {
        ServerConfiguration conf = new ServerConfiguration(TestBKConfiguration.newServerConfiguration());
        conf.setBookieId(name);

        File journalDir = createTempDir("bookie", journalPath);
        File ledgerDirNames = createTempDir("bookie", ledgerPath);

        int port = PortManager.nextFreePort();

        conf.setBookiePort(port);
        conf.setMetadataServiceUri("zk://127.0.0.1:" + zooKeeperServerUtil.getPort() + "/" + ledgerBaseRoot);

        for(int i = 0; i < ledgers.size(); i++) {
            conf.setJournalDirName(jornals.get(i).getAbsolutePath());
            conf.setLedgerDirNames(new String[]{ledgers.get(i).getAbsolutePath()});
            Assert.assertFalse(BookKeeperAdmin.initBookie(conf));
        }

        conf.setJournalDirName(journalDir.getAbsolutePath());
        conf.setLedgerDirNames(new String[]{ledgerDirNames.getAbsolutePath()});

        //Check if the path is valid
        Assert.assertEquals(expected, BookKeeperAdmin.initBookie(conf));

        if(expected) {
            try {
                BookieServer server = new BookieServer(conf);
                server.start();

                Collection<BookieId> ids = admin.getAllBookies();
                List<String> bookies = new ArrayList<>();
                for (BookieId id : ids) {
                    bookies.add(id.getId());
                }

                Assert.assertTrue(bookies.contains(name));
            } catch (BookieException.MetadataStoreException e) {
                if(ledgerBaseRoot.equals("ledgers")) {
                   Assert.fail();
                }
            }
        }
    }
}
