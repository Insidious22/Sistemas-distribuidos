package socket.conconexion.acelerografo;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Locale;

public class EstacionAcelTCP {
    private static final int PUERTO_DEFECTO = 7500;
    private static final long INTERVALO_DEFECTO_MS = 60000L;
    private static final String ID_ACEL_DEFECTO = "EC-UCON01";

    public static void main(String[] args) {
        try {
            String hostServidor = args.length >= 1 ? args[0] : leerTexto("IP del servidor TCP", "");
            if (hostServidor.trim().isEmpty()) {
                imprimirUso();
                return;
            }

            int puerto = args.length >= 2 ? Integer.parseInt(args[1]) : leerEntero("Puerto TCP", PUERTO_DEFECTO);
            String idAcel = args.length >= 3 ? args[2] : leerTexto("ID del acelerografo", ID_ACEL_DEFECTO);
            long intervaloMs = args.length >= 4
                    ? Long.parseLong(args[3])
                    : leerEntero("Intervalo entre envios en ms", (int) INTERVALO_DEFECTO_MS);
            int repeticiones = args.length >= 5 ? Integer.parseInt(args[4]) : leerEntero("Cantidad de envios", 1);

            for (int i = 0; i < repeticiones; i++) {
                enviarLectura(hostServidor, puerto, idAcel, i);
                esperarSiguienteEnvio(intervaloMs, i, repeticiones);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private static void enviarLectura(String hostServidor, int puerto, String idAcel, int indice) {
        double x = 0.020 + indice * 0.001;
        double y = -0.010 - indice * 0.001;
        double z = 0.980 + indice * 0.001;
        String payload = LecturaTriaxial.crearPayload(idAcel, x, y, z);

        try (Socket socket = new Socket(hostServidor, puerto);
                BufferedReader entrada = new BufferedReader(
                        new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
                PrintWriter salida = new PrintWriter(
                        new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8), true)) {

            long tiempoInicial = System.nanoTime();
            salida.println(payload);
            String ack = entrada.readLine();
            long tiempoFinal = System.nanoTime();
            double tTxMs = (tiempoFinal - tiempoInicial) / 1_000_000.0;

            System.out.println("ACK recibido: " + ack);
            System.out.printf(Locale.US, "[METRICA TCP] Registro enviado en: %.4f ms.%n", tTxMs);
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
        System.out.println("Uso: java -cp out socket.conconexion.acelerografo.EstacionAcelTCP <ipServidor> [puerto] [idAcel] [intervaloMs] [repeticiones]");
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
