package socket.conconexion.acelerografo;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

public class HiloDespachador implements Runnable {
    private final Socket socket;

    public HiloDespachador(Socket socket) {
        this.socket = socket;
    }

    @Override
    public void run() {
        try (Socket cliente = socket;
                BufferedReader entrada = new BufferedReader(
                        new InputStreamReader(cliente.getInputStream(), StandardCharsets.UTF_8));
                PrintWriter salida = new PrintWriter(
                        new OutputStreamWriter(cliente.getOutputStream(), StandardCharsets.UTF_8), true)) {

            String payload = entrada.readLine();
            if (payload == null || payload.trim().isEmpty()) {
                salida.println("ACK_ERROR:PAYLOAD_VACIO");
                return;
            }

            LecturaTriaxial lectura = LecturaTriaxial.desdePayload(payload);
            System.out.println("TCP recibido desde " + cliente.getRemoteSocketAddress() + " -> " + lectura);
            salida.println("ACK_TCP:" + lectura.getIdAcel() + ";RECIBIDO:" + Instant.now());
        } catch (Exception ex) {
            System.out.println("No se pudo procesar una lectura TCP.");
            ex.printStackTrace();
        }
    }
}
