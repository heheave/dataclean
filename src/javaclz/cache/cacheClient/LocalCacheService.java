package javaclz.cache.cacheClient;

import javaclz.cache.CEntry;
import javaclz.cache.CacheService;
import javaclz.cache.DeserEntrier;
import javaclz.cache.rabbitmq.RabbitCListener;
import javaclz.cache.rabbitmq.RabbitConsumer;
import org.apache.log4j.Logger;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * Created by xiaoke on 17-11-8.
 */
public class LocalCacheService<T extends DeserEntrier> implements CacheService {

    private static final Logger log = Logger.getLogger(LocalCacheService.class);

    private final String cacheDesc;

    private final InetSocketAddress isa;

    private final LRUCache<String, CEntry> lruCache;

    //private final ZkManager zkManager;

    //private final String zkSubPath;

    private final RabbitConsumer rabbitConsumer;

    private final T deser;

    private final boolean isTime;

    private final long autoExpiredTime;

    public LocalCacheService(String cacheDesc,
                             InetSocketAddress isa,
                             String mqHost,
                             int mqPort,
                             String mqTopic,
                             int lruCampacity,
                             T deser) {
        this(cacheDesc, isa, mqHost, mqPort, mqTopic,lruCampacity, false, 0L, deser);
    }

    public LocalCacheService(String cacheDesc,
                             InetSocketAddress isa,
                             String mqHost,
                             int mqPort,
                             String mqTopic,
                             int lruCampacity,
                             boolean isTime,
                             long autoExpiredTime,
                             T deser) {
        this.cacheDesc = cacheDesc;
        this.isa = isa;
        this.lruCache = lruCampacity <= 0 ? new LRUCache<String, CEntry>() : new LRUCache<String, CEntry>(lruCampacity);
        //this.zkManager = ZkManager.getInstance(zkUrl, zkTimeout);
        //this.zkSubPath = zkSubPath;
        this.rabbitConsumer = new RabbitConsumer(mqHost, mqPort, mqTopic);
        this.isTime = isTime;
        this.autoExpiredTime = autoExpiredTime;
        this.deser = deser;
    }

    public void startService() throws Exception{
//        ZkClient zkClient = zkManager.getClient();
//        final IZkDataListener dataListener = new IZkDataListener() {
//            public void handleDataChange(String s, Object o) throws Exception {
//                cacheExpired((String)o);
//            }
//
//            public void handleDataDeleted(String s) throws Exception {
//                log.warn("zk watch path has been deleted" + cacheDesc);
//            }
//        };
//        zkClient.subscribeDataChanges(zkSubPath, dataListener);
//        zkClient.exists(zkSubPath);
        rabbitConsumer.start(new RabbitCListener() {
            public void msgIn(String ctag, byte[] body) {
                String key = new String(body);
                cacheExpired(key);
            }
        });
    }

    public void stopService() {
        if(rabbitConsumer != null) {
            rabbitConsumer.stop();
        }
    }

    public CEntry get(String key) {
        CEntry tce = lruCache.get(key);
        if (tce == null) {
            tce = readWithMissCache(cacheDesc + REV_TOKEN1 + key);
            if (tce != null) {
                lruCache.put(key, tce);
            }
        } else if (isTime && (tce.getCreatedTime() + autoExpiredTime < System.currentTimeMillis())) {
            tce = readWithMissCache(cacheDesc + REV_TOKEN1 + key);
            lruCache.put(key, tce);
        }
        return tce;
    }

    private synchronized void cacheExpired(String key) {
        System.out.println(key);
        String[] info = key.split(REV_TOKEN1);
        if (info == null || info.length < 2 || !cacheDesc.equalsIgnoreCase(info[0])) {
            return;
        }
        lruCache.remove(info[1]);
    }

    private CEntry readWithMissCache(String key) {
        System.out.println("Query Remote Server for key " + key);
        Socket s = null;
        CEntry ce = null;
        try {
            s = new Socket();
            //s.setSoTimeout(2000);
            s.setTcpNoDelay(true);
            s.connect(isa);
            byte[] keyBytes = key.getBytes();
            int len = keyBytes.length;
            DataOutputStream dos = new DataOutputStream(s.getOutputStream());
            //System.out.println("Client write len: " + ((keyBytes == null) ? 0 : keyBytes.length));
            dos.writeInt(len);
            dos.write(keyBytes, 0, len);
            dos.flush();
            DataInputStream dis = new DataInputStream(s.getInputStream());
            int reMsgLen = dis.readInt();
            byte[] buf = new byte[reMsgLen];
            if (reMsgLen > 0) {
                //buf = new byte[reMsgLen];
                dis.readFully(buf, 0, reMsgLen);
            }
            ce = deser.fromBytes(buf);
            dis.close();
            dos.close();
        } catch (IOException e) {
            log.warn(String.format("Get entry from remote error: (%s , %s)", cacheDesc, key), e);
        } finally {
            try {
                if (s != null) {
                    s.close();
                }
            } catch (IOException e) {
                // do nothing
            }
        }
        return ce;
    }
}
