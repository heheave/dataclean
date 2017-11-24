package rpc;

import javaclz.JavaConfigure;
import javaclz.JavaV;
import org.apache.spark.SparkContext;
import rpc.common.Args;
import rpc.common.IService;
import rpc.common.RMType;
import rpc.common.Ret;
import sql.SqlJobMnager;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by xiaoke on 17-10-19.
 */
public class ServerImpl extends UnicastRemoteObject implements IService{

    private final SqlJobMnager sjm;

    private final ExecutorService es;

    private final Lock lock;

    private final int lockWaitTime;

    private final int maxTaskRunTime;

    public ServerImpl(SparkContext sparkContext, JavaConfigure jconf) throws RemoteException {
        super();
        this.sjm = null;//new SqlJobMnager(sparkContext, jconf);
        this.es = Executors.newSingleThreadExecutor();
        this.lock = new ReentrantLock();
        this.lockWaitTime = jconf.getIntOrElse(JavaV.SPARK_SQL_LOCK_WAIT);
        this.maxTaskRunTime = jconf.getIntOrElse(JavaV.SPARK_SQL_TASK_TIME);
    }

    public void shutdown() {
        es.shutdown();
        sjm.cancelAllJobs();
    }

    @Override
    public Ret rmcall(Args args) throws RemoteException {
        RMType type = args.getRmType();
        switch (type) {
            case SQL: {
                return sqlJob(args);
            }

            case QUERY: {
                return queryJob(args);
            }

            default: {
                return unkownJob(args);
            }
        }
    }

    private boolean aquire(int sec, TimeUnit tu) {
        try {
            if (lock.tryLock(sec, tu)) {
                return true;
            } else {
                return false;
            }
        } catch (InterruptedException e) {
            return false;
        }
    }

    private void finish() {
        lock.unlock();
    }

    private Ret sqlJob(final Args args) {
        if (aquire(lockWaitTime, TimeUnit.MILLISECONDS)) {
            try {
                Future<String[]> f = es.submit(new Callable() {
                    @Override
                    public String[] call() throws Exception {
                        return sjm.submitNewJob(args.getAttrs());
                    }
                });
                String[] res = f.get(maxTaskRunTime, TimeUnit.MILLISECONDS);
                return Ret.s(res);
            } catch (Exception e) {
                sjm.cancelAllJobs();
                return Ret.e(e.getMessage());
            } finally {
                finish();
            }
        } else {
            return Ret.f("Platform is busy now");
        }
    }

    private Ret queryJob(Args args) {
        return Ret.s("Query Job Success");
    }

    private Ret unkownJob(Args args) {
        return Ret.f("Unkown Job");
    }
}
