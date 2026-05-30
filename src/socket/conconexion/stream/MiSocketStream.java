package socket.conconexion.stream;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;

public class MiSocketStream implements AutoCloseable {
    private final Socket socket;
    private BufferedReader entrada;
    private PrintWriter salida;

    MiSocketStream(String maquinaAceptadora, int puertoAceptador) throws SocketException, IOException {
        socket = new Socket(maquinaAceptadora, puertoAceptador);
        establecerFlujos();
    }

    MiSocketStream(Socket socket) throws IOException {
        this.socket = socket;
        establecerFlujos();
    }

    private void establecerFlujos() throws IOException {
        InputStream flujoEntrada = socket.getInputStream();
        entrada = new BufferedReader(new InputStreamReader(flujoEntrada, StandardCharsets.UTF_8));
        OutputStream flujoSalida = socket.getOutputStream();
        salida = new PrintWriter(new OutputStreamWriter(flujoSalida, StandardCharsets.UTF_8));
    }

    public void enviaMensaje(String mensaje) {
        salida.println(mensaje);
        salida.flush();
    }

    public String recibeMensaje() throws IOException {
        return entrada.readLine();
    }

    @Override
    public void close() throws IOException {
        socket.close();
    }
}
