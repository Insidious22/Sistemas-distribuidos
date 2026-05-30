package socket.sinconexion.simplex;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Receptor {
    private static final int MAX_UDP_PAYLOAD = 65507;
    private static final Pattern TELEMETRIA = Pattern.compile("TEMP:([0-9]+(?:\\.[0-9]+)?);HUM:([0-9]+)%");

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Uso: java socket.sinconexion.simplex.Receptor <puerto> [cantidad]");
            return;
        }

        int puerto = Integer.parseInt(args[0]);
        int cantidad = args.length >= 2 ? Integer.parseInt(args[1]) : -1;
        int recibidos = 0;
        double sumaTemperatura = 0;
        double sumaHumedad = 0;
        int lecturasTelemetria = 0;

        try (DatagramSocket miSocket = new DatagramSocket(puerto)) {
            System.out.println("Receptor UDP libre escuchando en puerto " + puerto);

            while (cantidad < 0 || recibidos < cantidad) {
                byte[] almacen = new byte[MAX_UDP_PAYLOAD];
                DatagramPacket datagrama = new DatagramPacket(almacen, almacen.length);
                miSocket.receive(datagrama);
                recibidos++;

                String mensaje = new String(datagrama.getData(), datagrama.getOffset(), datagrama.getLength(),
                        StandardCharsets.UTF_8);
                InetAddress direccionEmisor = datagrama.getAddress();
                int puertoEmisor = datagrama.getPort();
                String origen = direccionEmisor.getHostAddress() + ":" + puertoEmisor;

                System.out.println("Origen: " + origen);
                System.out.println("Mensaje completo: " + mensaje);

                Matcher matcher = TELEMETRIA.matcher(mensaje.trim());
                if (matcher.matches()) {
                    double temperatura = Double.parseDouble(matcher.group(1));
                    double humedad = Double.parseDouble(matcher.group(2));
                    sumaTemperatura += temperatura;
                    sumaHumedad += humedad;
                    lecturasTelemetria++;

                    System.out.printf(Locale.US, "Promedios -> TEMP: %.2f; HUM: %.2f%%%n",
                            sumaTemperatura / lecturasTelemetria,
                            sumaHumedad / lecturasTelemetria);
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
