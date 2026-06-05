package rmi.sum;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface ServicioSum extends Remote {
    int sumar(int primerNumero, int segundoNumero) throws RemoteException;
}
