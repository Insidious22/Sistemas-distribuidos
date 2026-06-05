package rmi.acelerografo;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Locale;

public class EstacionAcelRMI {
    private static final int PUERTO_DEFECTO = 1099;
    private static final String SERVICIO_DEFECTO = "acelerografo";
    private static final long INTERVALO_DEFECTO_MS = 60000L;
    private static final String ID_ACEL_DEFECTO = "EC-UCON01";

    public static void main(String[] args) {
        try {
            String hostServidor = args.length >= 1 ? args[0] : leerTexto("IP del servidor RMI", "");
            if (hostServidor.trim().isEmpty()) {
                imprimirUso();
                return;
            }

            int puerto = args.length >= 2 ? Integer.parseInt(args[1]) : leerEntero("Puerto RMI", PUERTO_DEFECTO);
            String nombreServicio = args.length >= 3
                    ? args[2]
                    : leerTexto("Nombre del servicio RMI", SERVICIO_DEFECTO);
            String idAcel = args.length >= 4 ? args[3] : leerTexto("ID del acelerografo", ID_ACEL_DEFECTO);
            long intervaloMs = args.length >= 5
                    ? Long.parseLong(args[4])
                    : leerEntero("Intervalo entre invocaciones en ms", (int) INTERVALO_DEFECTO_MS);
            int repeticiones = args.length >= 6 ? Integer.parseInt(args[5]) : leerEntero("Cantidad de envios", 1);

            Registry registro = LocateRegistry.getRegistry(hostServidor, puerto);
            ServicioAcelerografo servicio = (ServicioAcelerografo) registro.lookup(nombreServicio);

            for (int i = 0; i < repeticiones; i++) {
                double x = 0.020 + i * 0.001;
                double y = -0.010 - i * 0.001;
                double z = 0.980 + i * 0.001;

                long tiempoInicial = System.nanoTime();
                servicio.registrarLecturaTriaxial(idAcel, x, y, z);
                long tiempoFinal = System.nanoTime();
                double tTxMs = (tiempoFinal - tiempoInicial) / 1_000_000.0;

                System.out.printf(Locale.US, "[METRICA RMI] Invocacion remota ejecutada en: %.4f ms.%n", tTxMs);
                esperarSiguienteEnvio(intervaloMs, i, repeticiones);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private static void esperarSiguienteEnvio(long intervaloMs, int indice, int repeticiones) {
        if (indice + 1 >= repeticiones) {
            return;
        }
        try {
            Thread.sleep(intervaloMs);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }

    private static void imprimirUso() {
        System.out.println("Uso: java -cp out rmi.acelerografo.EstacionAcelRMI <ipServidor> [puerto] [servicio] [idAcel] [intervaloMs] [repeticiones]");
    }

    private static String leerTexto(String mensaje, String valorDefecto) throws Exception {
        String etiquetaDefecto = valorDefecto == null || valorDefecto.isEmpty() ? "" : " [" + valorDefecto + "]";
        System.out.print(mensaje + etiquetaDefecto + ": ");
        String valor = new BufferedReader(new InputStreamReader(System.in)).readLine();
        if (valor == null || valor.trim().isEmpty()) {
            return valorDefecto;
        }
        return valor.trim();
    }

    private static int leerEntero(String mensaje, int valorDefecto) throws Exception {
        return Integer.parseInt(leerTexto(mensaje, Integer.toString(valorDefecto)));
    }
}
