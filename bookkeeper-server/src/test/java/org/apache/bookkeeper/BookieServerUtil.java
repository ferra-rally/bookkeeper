package org.apache.bookkeeper;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.replication.ReplicationException;
import org.apache.bookkeeper.tls.SecurityException;
import org.apache.bookkeeper.util.PortManager;
import org.apache.zookeeper.KeeperException;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import static org.apache.bookkeeper.util.IOUtils.createTempDir;

public class BookieServerUtil {
    private final ServerConfiguration baseConf = TestBKConfiguration.newServerConfiguration();

    public void startBookies(int num, String zkaddr) throws IOException, ReplicationException.CompatibilityException, ReplicationException.UnavailableException, SecurityException, InterruptedException, KeeperException, BookieException {
        //Create temp directory for bookie
        //File f = createTempDir("bookie", "test");
        //System.out.println("A" + zkaddr + "A");
        //baseConf.setMetadataServiceUri(zkaddr);
        baseConf.setMetadataServiceUri("zk://127.0.0.1:21810/ledgers");

        for(int i = 0; i < num; i++) {
            ServerConfiguration conf = new ServerConfiguration(baseConf);
            //conf.setBookieId(UUID.randomUUID().toString());
            conf.setUseHostNameAsBookieID(true);

            File journalDir = createTempDir("bookie", "journal" + i);
            File ledgerDirNames = createTempDir("bookie", "ledger" + i);
            conf.setJournalDirName(journalDir.getPath());

            conf.setLedgerDirNames(new String[] {ledgerDirNames.getPath()});
            int port = PortManager.nextFreePort();
            conf.setBookiePort(port);


            BookieServer server = new BookieServer(conf);
            //BookieId address = Bookie.getBookieId(conf);

            server.start();
            System.out.println("Starting bookie with " + server.getLocalAddress().getPort());
        }
    }
}
