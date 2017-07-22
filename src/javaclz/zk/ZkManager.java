package javaclz.zk;

import org.I0Itec.zkclient.*;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.commons.io.Charsets;

/**
 * Created by xiaoke on 17-7-22.
 */
public class ZkManager {

    private static class MyZkSerializer implements ZkSerializer {
        @Override
        public Object deserialize(byte[] bytes) throws ZkMarshallingError {
            return new String(bytes, Charsets.UTF_8);
        }

        @Override
        public byte[] serialize(Object obj) throws ZkMarshallingError {
            return String.valueOf(obj).getBytes(Charsets.UTF_8);
        }
    }

    private static final ZkSerializer defaultSer = new MyZkSerializer();

    private static final int defaultTimeout = 1000;

    private final String serverUrl;

    private final int sessionTimeout;

    private final int connectionTimeout;

    private final ZkSerializer zkSer;

    private ZkClient zkClient;

    public ZkManager(String serverUrl, int sessionTimeout, int connectionTimeout, ZkSerializer zkSer) {
        this.serverUrl = serverUrl;
        this.sessionTimeout = sessionTimeout;
        this.connectionTimeout = connectionTimeout;
        this.zkSer = zkSer;
        init();
    }

    public ZkManager(String serverUrl, ZkSerializer zkSer) {
        this(serverUrl, defaultTimeout, defaultTimeout, zkSer);
    }

    public ZkManager(String serverUrl, int timeout) {
        this(serverUrl, timeout, timeout, defaultSer);
    }

    public ZkManager(String serverUrl, int timeout, ZkSerializer zkSer) {
        this(serverUrl, timeout, timeout, zkSer);
    }

    public ZkManager(String serverUrl) {
        this(serverUrl, defaultTimeout, defaultSer);
    }

    private void init() {
        zkClient = new ZkClient(serverUrl, sessionTimeout, connectionTimeout, zkSer);
    }

    public ZkClient getClient() {
        zkClient.waitUntilConnected();
        return zkClient;
    }

    public void close() {
        ZkClient zkClient = getClient();
        zkClient.close();
    }

}
