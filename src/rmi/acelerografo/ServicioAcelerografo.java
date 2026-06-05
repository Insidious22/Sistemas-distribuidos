package rmi.acelerografo;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface ServicioAcelerografo extends Remote {
    void registrarLecturaTriaxial(String idAcel, double x, double y, double z) throws RemoteException;
}
