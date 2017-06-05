package javaclz.zk;

import org.apache.log4j.Logger;
import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Created by xiaoke on 17-6-4.
 */
public class ZkClient {

    private static final Logger log = Logger.getLogger(ZkClient.class);

    private static final String idFormat = "%010d";

    private CountDownLatch latch = null;

    private ZooKeeper zk = null;

    private String host;

    private int port;

    private int timeout;

    public interface ZkReconnected {
        void reconnected(ZooKeeper zk);
    }

    private final ZkReconnected reConnectRun;

    public ZkClient(String host, int port, int timeout, ZkReconnected reConnectRun) {
        this.host = host;
        this.port = port;
        this.timeout = timeout;
        this.reConnectRun = reConnectRun;
        getZkClient(host, port, false);
    }


    public ZooKeeper zk() throws InterruptedException {
        if (zk == null) {
            if (latch != null) {
                latch.await(timeout, TimeUnit.MILLISECONDS);
            }
        }
        return zk;
    }

    private void getZkClient(String host, int port, boolean isRec) {
        if (zk == null) {
            synchronized (this) {
                if (zk == null) {
                    try {
                        latch = new CountDownLatch(1);
                        buildClient(host, port, isRec);
                    } catch (Exception e) {
                        log.warn("Create zk client error", e);
                    }
                }
            }
        }
    }

    private void buildClient(String h, int p, boolean isRec) throws IOException, InterruptedException, KeeperException {
        String hostUrl = h + ":" + p;
        zk = new ZooKeeper(hostUrl, timeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
                    if (latch != null) {
                        latch.countDown();
                    }
                } else if (watchedEvent.getState() == Event.KeeperState.Expired) {
                    close();
                    getZkClient(host, port, true);
                } else {
                    log.info(watchedEvent.getState());
                }
            }
        });
        try {
            latch.await(timeout, TimeUnit.MILLISECONDS);
        } finally {
            latch = null;
        }

        if (isRec && reConnectRun != null) {
            reConnectRun.reconnected(zk);
        }
    }

    public void close() {
        if (zk != null) {
            try {
                zk.close();
                zk = null;
            } catch (Exception e) {
                log.warn("Close zk client error", e);
            }
        }
    }
}
