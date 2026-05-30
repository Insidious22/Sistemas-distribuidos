package socket.conconexion.stream;

import java.net.ServerSocket;
import java.util.HashMap;
import java.util.Map;

public class ServidorBanco {
    private static final Map<String, Double> SALDOS = new HashMap<String, Double>();

    static {
        SALDOS.put("TOKEN123", 1000.00);
        SALDOS.put("TOKEN456", 250.75);
    }

    public static void main(String[] args) {
        int puerto = args.length >= 1 ? Integer.parseInt(args[0]) : 5001;

        try (ServerSocket servidor = new ServerSocket(puerto)) {
            System.out.println("Servidor bancario escuchando en puerto " + puerto);

            while (true) {
                try (MiSocketStream sesion = new MiSocketStream(servidor.accept())) {
                    atenderSesion(sesion);
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private static void atenderSesion(MiSocketStream sesion) throws Exception {
        sesion.enviaMensaje("BIENVENIDO");

        String token = sesion.recibeMensaje();
        if (token == null || !token.startsWith("TOKEN:")) {
            sesion.enviaMensaje("ERROR_PROTOCOLO");
            return;
        }

        String valorToken = token.substring("TOKEN:".length());
        Double saldo = SALDOS.get(valorToken);
        if (saldo == null) {
            sesion.enviaMensaje("AUTH_RECHAZADO");
            return;
        }

        sesion.enviaMensaje("AUTH_OK");
        String solicitud = sesion.recibeMensaje();
        if (solicitud == null || !solicitud.startsWith("RETIRO:")) {
            sesion.enviaMensaje("ERROR_PROTOCOLO");
            return;
        }

        double monto = Double.parseDouble(solicitud.substring("RETIRO:".length()));
        if (monto <= 0 || monto > saldo) {
            sesion.enviaMensaje(String.format(java.util.Locale.US, "RETIRO_RECHAZADO|SALDO=%.2f", saldo));
            return;
        }

        double nuevoSaldo = saldo - monto;
        SALDOS.put(valorToken, nuevoSaldo);
        sesion.enviaMensaje(String.format(java.util.Locale.US, "RETIRO_OK|SALDO=%.2f", nuevoSaldo));
    }
}
