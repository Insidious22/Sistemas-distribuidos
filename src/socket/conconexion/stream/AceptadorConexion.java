package socket.conconexion.stream;

import java.net.ServerSocket;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class AceptadorConexion {
    private static final String REPORTE = "AUDITORIA_FINANCIERA|ACTIVO=125000.50|PASIVO=78500.25|PATRIMONIO=46500.25";

    public static void main(String[] args) {
        int numPuerto = args.length >= 1 ? Integer.parseInt(args[0]) : 5000;

        try (ServerSocket socketConexion = new ServerSocket(numPuerto)) {
            System.out.println("Servidor de reportes escuchando en puerto " + numPuerto);

            while (true) {
                System.out.println("Preparado para aceptar una conexion.");
                try (MiSocketStream socketDatos = new MiSocketStream(socketConexion.accept())) {
                    System.out.println("Conexion aceptada.");
                    String solicitud = socketDatos.recibeMensaje();
                    System.out.println("Solicitud recibida: " + solicitud);
                    socketDatos.enviaMensaje(generarReporte());
                    System.out.println("Reporte enviado y socket de datos cerrado.");
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
