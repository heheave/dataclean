package rpc.common;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * Created by xiaoke on 17-10-19.
 */
public interface IService extends Remote {

    Ret rmcall(Args args) throws RemoteException;

}
