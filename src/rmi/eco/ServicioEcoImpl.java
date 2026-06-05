package rmi.eco;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Locale;

public class ServicioEcoImpl extends UnicastRemoteObject implements ServicioEco {
    private static final long serialVersionUID = 1L;

    public ServicioEcoImpl() throws RemoteException {
        super();
    }

    @Override
    public String eco(String mensaje) throws RemoteException {
        return "desde SRV: " + mensaje.toUpperCase(Locale.ROOT);
    }
}
