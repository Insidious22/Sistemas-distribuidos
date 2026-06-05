package rmi.eco;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface ServicioEco extends Remote {
    String eco(String mensaje) throws RemoteException;
}
