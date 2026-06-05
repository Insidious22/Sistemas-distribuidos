package rmi.acelerografo;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.time.Instant;
import java.util.Locale;

public class ServicioAcelerografoImpl extends UnicastRemoteObject implements ServicioAcelerografo {
    private static final long serialVersionUID = 1L;

    public ServicioAcelerografoImpl() throws RemoteException {
        super();
    }

    @Override
    public void registrarLecturaTriaxial(String idAcel, double x, double y, double z) throws RemoteException {
        System.out.printf(Locale.US, "RMI recibido -> %s X=%+.3fG Y=%+.3fG Z=%+.3fG TIMESTAMP=%s%n",
                idAcel, x, y, z, Instant.now());
    }
}
