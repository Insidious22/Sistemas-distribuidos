package socket.conconexion.stream;

public class SolicitanteConexion {
    public static void main(String[] args) {
        String maquinaAceptadora = args.length >= 1 ? args[0] : "localhost";
        int puertoAceptador = args.length >= 2 ? Integer.parseInt(args[1]) : 5000;
        String solicitud = args.length >= 3 ? args[2] : "DESCARGAR_REPORTE_AUDITORIA";

        try (MiSocketStream miSocket = new MiSocketStream(maquinaAceptadora, puertoAceptador)) {
            System.out.println("Solicitud de conexion concedida con " + maquinaAceptadora + ":" + puertoAceptador);
            miSocket.enviaMensaje(solicitud);
            String mensaje = miSocket.recibeMensaje();
            System.out.println("Reporte recibido:");
            System.out.println(mensaje);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
