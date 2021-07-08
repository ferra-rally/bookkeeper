package org.apache.bookkeeper.util;

import org.apache.bookkeeper.TestBKConfiguration;
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
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.apache.bookkeeper.util.IOUtils.createTempDir;

public class BookieServerUtil {
    private final ServerConfiguration baseConf = TestBKConfiguration.newServerConfiguration();
    private ZooKeeperServerUtil zooKeeperServerUtil;
    private List<BookieServer> bookies = new ArrayList<>();

    public BookieServerUtil(ZooKeeperServerUtil zooKeeperServerUtil) {
        this.zooKeeperServerUtil = zooKeeperServerUtil;
        System.out.println(zooKeeperServerUtil.getZooKeeperAddress());
    }

    public void startBookies(int num) {
        //Create temp directory for bookie
        //File f = createTempDir("bookie", "test");
        //System.out.println("A" + zkaddr + "A");
        //baseConf.setMetadataServiceUri(zkaddr);
        baseConf.setMetadataServiceUri("zk://127.0.0.1:" + 21810 + "/ledgers");

        for(int i = 0; i < num; i++) {
            ServerConfiguration conf = new ServerConfiguration(baseConf);
            //conf.setBookieId(UUID.randomUUID().toString());
            conf.setUseHostNameAsBookieID(true);

            try {

                File journalDir = createTempDir("bookie", "journal" + i);
                File ledgerDirNames = createTempDir("bookie", "ledger" + i);
                conf.setJournalDirName(journalDir.getPath());

                conf.setLedgerDirNames(new String[]{ledgerDirNames.getPath()});
                int port = PortManager.nextFreePort();
                //int port = 0;
                conf.setBookiePort(port);

                BookieServer server = new BookieServer(conf);
                bookies.add(server);
                //BookieId address = Bookie.getBookieId(conf);

                server.start();
                System.out.println("Starting bookie with " + server.getLocalAddress().getPort());
            } catch (ReplicationException.CompatibilityException e) {
                e.printStackTrace();
            } catch (UnknownHostException e) {
                e.printStackTrace();
            } catch (ReplicationException.UnavailableException e) {
                e.printStackTrace();
            } catch (SecurityException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (BookieException e) {
                e.printStackTrace();
            }
        }
    }

    public void stopBookies(int num) {
        int x;
        if(bookies.size() < num) {
            x = bookies.size();
        } else {
            x = num;
        }

        for(int i = 0; i < x; i++) {
            System.out.println("NUMBER OF BOOKIES: " + bookies.size());
            System.out.println("SHUTTING DOWN BOOKIE: " + i);
            bookies.get(i).shutdown();
            System.out.println("ALIVE BOOKIES: " + numberOfAliveBookies());
        }
    }

    public int numberOfAliveBookies() {
        int x = 0;
        for(int i = 0; i < bookies.size(); i++) {
            if(bookies.get(i).isRunning()) {
                x++;
            }
        }

        return x;
    }
}
