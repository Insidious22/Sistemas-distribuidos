package socket.conconexion.unilateral;

import java.net.InetAddress;
import java.net.SocketTimeoutException;

public class Emisor {
    private static final int TOTAL_COMANDOS = 10;
    private static final int TIMEOUT_ACK_MS = 5000;

    public static void main(String[] args) {
        if (args.length < 4) {
            System.out.println("Uso: java socket.conconexion.unilateral.Emisor <hostReceptor> <puertoReceptor> <miPuerto> <comando>");
            return;
        }

        try {
            InetAddress maquinaReceptora = InetAddress.getByName(args[0]);
            int puertoReceptor = Integer.parseInt(args[1]);
            int miPuerto = Integer.parseInt(args[2]);
            String comando = args[3];

            try (MiSocketDatagrama miSocket = new MiSocketDatagrama(miPuerto)) {
                miSocket.connect(maquinaReceptora, puertoReceptor);
                miSocket.setSoTimeout(TIMEOUT_ACK_MS);
                System.out.println("Socket UDP conectado a " + maquinaReceptora.getHostAddress() + ":" + puertoReceptor);

                for (int i = 1; i <= TOTAL_COMANDOS; i++) {
                    String mensaje = comando + "#" + i;
                    miSocket.enviaMensaje(maquinaReceptora, puertoReceptor, mensaje);
                    System.out.println("Comando enviado: " + mensaje);
                }

                try {
                    String respuesta = miSocket.recibeMensaje();
                    System.out.println("ACK recibido: " + respuesta);
                    System.out.println("Origen del ACK: " + miSocket.obtenerIPRemitente() + ":" + miSocket.obtenerPuertoRemitente());
                } catch (SocketTimeoutException ex) {
                    System.out.println("No llego ACK dentro de " + TIMEOUT_ACK_MS + " ms. Posible perdida UDP o receptor esperando datagramas.");
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
