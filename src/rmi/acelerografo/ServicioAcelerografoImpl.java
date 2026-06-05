package rmi.acelerografo;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.time.Instant;
import java.util.Locale;
import telemetria.RegistroMetricas;

public class ServicioAcelerografoImpl extends UnicastRemoteObject implements ServicioAcelerografo {
    private static final long serialVersionUID = 1L;

    public ServicioAcelerografoImpl() throws RemoteException {
        super();
    }

    @Override
    public void registrarLecturaTriaxial(String idAcel, double x, double y, double z) throws RemoteException {
        long tiempoInicial = System.nanoTime();
        String recibido = Instant.now().toString();
        System.out.printf(Locale.US, "RMI recibido -> %s X=%+.3fG Y=%+.3fG Z=%+.3fG TIMESTAMP=%s%n",
                idAcel, x, y, z, recibido);
        long tiempoFinal = System.nanoTime();
        double procesamientoMs = (tiempoFinal - tiempoInicial) / 1_000_000.0;
        RegistroMetricas.escribir("servidor_rmi_metricas.log",
                RegistroMetricas.linea("RMI_SERVIDOR", "idAcel=" + idAcel,
                        "procesamientoMs=" + RegistroMetricas.ms(procesamientoMs), "x=" + x, "y=" + y, "z=" + z,
                        "recibido=" + recibido));
    }
}
