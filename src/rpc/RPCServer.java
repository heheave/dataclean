package rpc;

import javaclz.JavaConfigure;
import javaclz.JavaV;
import org.apache.spark.SparkContext;
import rpc.common.IService;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

/**
 * Created by xiaoke on 17-10-19.
 */
public class RPCServer {

    private volatile boolean isRunning;

    private volatile ServerImpl server;

    public void start(SparkContext sparkContext, JavaConfigure jconf) throws Exception {
        if (isRunning) return;
        isRunning = true;
        String bindName = jconf.getStringOrElse(JavaV.RPC_BIND_NAME);
        int bindPort = jconf.getIntOrElse(JavaV.RPC_BIND_PORT);
        Registry registry = LocateRegistry.createRegistry(bindPort);
        IService server = new ServerImpl(sparkContext, jconf);
        registry.bind(bindName, server);
    }

    public void stop() {
        isRunning = false;
        if (server != null) {
            server.shutdown();
        }
    }

    public static void main(String[] args) {
        RPCServer server = new RPCServer();
        try {
            server.start(null, new JavaConfigure());
            //Thread.sleep(1000000);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
