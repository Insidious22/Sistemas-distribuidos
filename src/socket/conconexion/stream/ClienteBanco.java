package socket.conconexion.stream;

public class ClienteBanco {
    public static void main(String[] args) {
        String host = args.length >= 1 ? args[0] : "localhost";
        int puerto = args.length >= 2 ? Integer.parseInt(args[1]) : 5001;
        String token = args.length >= 3 ? args[2] : "TOKEN123";
        String monto = args.length >= 4 ? args[3] : "100.00";

        try (MiSocketStream sesion = new MiSocketStream(host, puerto)) {
            System.out.println("Servidor: " + sesion.recibeMensaje());
            sesion.enviaMensaje("TOKEN:" + token);
            String validacion = sesion.recibeMensaje();
            System.out.println("Servidor: " + validacion);

            if (!"AUTH_OK".equals(validacion)) {
                return;
            }

            sesion.enviaMensaje("RETIRO:" + monto);
            System.out.println("Servidor: " + sesion.recibeMensaje());
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
