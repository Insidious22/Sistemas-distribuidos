package socket.conconexion.unilateral;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;

public class MiSocketDatagrama extends DatagramSocket {
    private static final int MAX_UDP_PAYLOAD = 65507;
    private DatagramPacket ultimoDatagrama;

    MiSocketDatagrama(int numPuerto) throws SocketException {
        super(numPuerto);
    }

    public void enviaMensaje(InetAddress maquinaReceptora, int puertoReceptor, String mensaje) throws IOException {
        byte[] almacenEnvio = mensaje.getBytes(StandardCharsets.UTF_8);
        DatagramPacket datagrama = new DatagramPacket(almacenEnvio, almacenEnvio.length, maquinaReceptora,
                puertoReceptor);
        send(datagrama);
    }

    public String recibeMensaje() throws IOException {
        byte[] almacenRecepcion = new byte[MAX_UDP_PAYLOAD];
        ultimoDatagrama = new DatagramPacket(almacenRecepcion, almacenRecepcion.length);
        receive(ultimoDatagrama);
        return new String(ultimoDatagrama.getData(), ultimoDatagrama.getOffset(), ultimoDatagrama.getLength(),
                StandardCharsets.UTF_8);
    }

    public String obtenerIPRemitente() {
        return ultimoDatagrama == null ? null : ultimoDatagrama.getAddress().getHostAddress();
    }

    public int obtenerPuertoRemitente() {
        return ultimoDatagrama == null ? -1 : ultimoDatagrama.getPort();
    }
}
