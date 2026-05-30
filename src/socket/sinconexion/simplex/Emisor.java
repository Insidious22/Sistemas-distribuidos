package socket.sinconexion.simplex;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Random;

public class Emisor {
    private static final long INTERVALO_DEFECTO_MS = 2000;
    private static final Random ALEATORIO = new Random();

    public static void main(String[] args) {
        if (args.length < 2) {
            imprimirUso();
            return;
        }

        try {
            InetAddress maquinaReceptora = InetAddress.getByName(args[0]);
            int puertoReceptor = Integer.parseInt(args[1]);
            String modo = args.length >= 3 ? args[2].toLowerCase(Locale.ROOT) : "telemetria";

            try (DatagramSocket miSocket = new DatagramSocket()) {
                if ("alerta".equals(modo)) {
                    String mensaje = args.length >= 4 ? args[3] : "ALERTA_INCENDIO_SECTOR_B";
                    enviar(miSocket, maquinaReceptora, puertoReceptor, mensaje);
                    System.out.println("Alerta enviada: " + mensaje);
                    return;
                }

                long intervaloMs = args.length >= 4 ? Long.parseLong(args[3]) : INTERVALO_DEFECTO_MS;
                int cantidad = args.length >= 5 ? Integer.parseInt(args[4]) : -1;
                int enviados = 0;

                while (cantidad < 0 || enviados < cantidad) {
                    String mensaje = generarTelemetria();
                    enviar(miSocket, maquinaReceptora, puertoReceptor, mensaje);
                    System.out.println("Telemetria enviada: " + mensaje);
                    enviados++;
                    Thread.sleep(intervaloMs);
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private static void enviar(DatagramSocket socket, InetAddress destino, int puerto, String mensaje)
            throws Exception {
        byte[] almacen = mensaje.getBytes(StandardCharsets.UTF_8);
        DatagramPacket datagrama = new DatagramPacket(almacen, almacen.length, destino, puerto);
        socket.send(datagrama);
    }

    private static String generarTelemetria() {
        double temperatura = 18.0 + ALEATORIO.nextDouble() * 17.0;
        int humedad = 35 + ALEATORIO.nextInt(61);
        return String.format(Locale.US, "TEMP:%.1f;HUM:%d%%", temperatura, humedad);
    }

    private static void imprimirUso() {
        System.out.println("Uso telemetria: java socket.sinconexion.simplex.Emisor <host> <puerto> [telemetria] [intervaloMs] [cantidad]");
        System.out.println("Uso alerta:     java socket.sinconexion.simplex.Emisor <host> <puerto> alerta [mensaje]");
    }
}
