package rmi.sum;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class ServicioSumImpl extends UnicastRemoteObject implements ServicioSum {
    private static final long serialVersionUID = 1L;

    public ServicioSumImpl() throws RemoteException {
        super();
    }

    @Override
    public int sumar(int primerNumero, int segundoNumero) throws RemoteException {
        return primerNumero + segundoNumero;
    }
}
