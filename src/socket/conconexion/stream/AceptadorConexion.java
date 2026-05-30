package socket.conconexion.stream;

import java.net.ServerSocket;
import java.net.SocketTimeoutException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class AceptadorConexion {
    private static final String REPORTE = "AUDITORIA_FINANCIERA|ACTIVO=125000.50|PASIVO=78500.25|PATRIMONIO=46500.25";
    private static final int TIMEOUT_CLIENTE_MS = 15000;

    public static void main(String[] args) {
        int numPuerto = args.length >= 1 ? Integer.parseInt(args[0]) : 5000;

        try (ServerSocket socketConexion = new ServerSocket(numPuerto)) {
            System.out.println("Servidor de reportes escuchando en puerto " + numPuerto);
            System.out.println("Deje esta ventana abierta. Presione Ctrl + C para detener el servidor.");

            while (true) {
                System.out.println("Preparado para aceptar una conexion.");
                try (MiSocketStream socketDatos = new MiSocketStream(socketConexion.accept())) {
                    socketDatos.setSoTimeout(TIMEOUT_CLIENTE_MS);
                    System.out.println("Conexion aceptada desde " + socketDatos.getRemoteSocketAddress());
                    String solicitud = socketDatos.recibeMensaje();

                    if (solicitud == null || solicitud.trim().isEmpty()) {
                        System.out.println("El cliente cerro la conexion sin enviar solicitud.");
                        continue;
                    }

                    System.out.println("Solicitud recibida: " + solicitud);
                    socketDatos.enviaMensaje(generarReporte());
                    System.out.println("Reporte enviado y socket de datos cerrado.");
                } catch (SocketTimeoutException ex) {
                    System.out.println("Cliente desconectado por inactividad despues de " + TIMEOUT_CLIENTE_MS + " ms.");
                } catch (Exception ex) {
                    System.out.println("No se pudo atender una conexion, el servidor seguira escuchando.");
                    ex.printStackTrace();
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private static String generarReporte() {
        String fecha = LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        return REPORTE + "|GENERADO=" + fecha;
    }
}
