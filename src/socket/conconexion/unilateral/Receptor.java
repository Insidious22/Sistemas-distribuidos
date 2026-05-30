package socket.conconexion.unilateral;

import java.net.InetAddress;

public class Receptor {
    private static final int TOTAL_COMANDOS = 10;
    private static final String ACK = "CALIBRADO_OK";

    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Uso: java socket.conconexion.unilateral.Receptor <hostEmisor> <puertoEmisor> <miPuerto>");
            return;
        }

        try {
            InetAddress maquinaEmisora = InetAddress.getByName(args[0]);
            int puertoEmisor = Integer.parseInt(args[1]);
            int miPuerto = Integer.parseInt(args[2]);

            try (MiSocketDatagrama miSocket = new MiSocketDatagrama(miPuerto)) {
                miSocket.connect(maquinaEmisora, puertoEmisor);
                System.out.println("Receptor UDP conectado solo a " + maquinaEmisora.getHostAddress() + ":" + puertoEmisor);

                for (int i = 1; i <= TOTAL_COMANDOS; i++) {
                    String mensaje = miSocket.recibeMensaje();
                    System.out.println("Comando " + i + " recibido: " + mensaje);
                    System.out.println("Origen aceptado: " + miSocket.obtenerIPRemitente() + ":" + miSocket.obtenerPuertoRemitente());
                }

                miSocket.enviaMensaje(maquinaEmisora, puertoEmisor, ACK);
                System.out.println("ACK enviado: " + ACK);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
