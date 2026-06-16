package socket.conconexion.acelerografo;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import telemetria.RegistroMetricas;

public class SimuladorSismoTCP {
    public static void main(String[] args) {
        if (args.length < 4) {
            System.out.println("Uso: java -cp out socket.conconexion.acelerografo.SimuladorSismoTCP <ipServidor> <puerto> <idEvento> <duracionMs> [retardoClienteMs]");
            return;
        }

        String host = args[0];
        int puerto = Integer.parseInt(args[1]);
        String idEvento = args[2];
        long duracionMs = Long.parseLong(args[3]);
        long retardoClienteMs = args.length >= 5 ? Long.parseLong(args[4]) : duracionMs;

        try (Socket socket = new Socket(host, puerto);
                BufferedReader entrada = new BufferedReader(
                        new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
                PrintWriter salida = new PrintWriter(
                        new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8), true)) {

            String comando = "SIMULAR_SISMO;ID_EVENTO:" + idEvento + ";DURACION_MS:" + duracionMs
                    + ";RETARDO_CLIENTE_MS:" + retardoClienteMs;
            salida.println(comando);
            String ack = entrada.readLine();
            System.out.println("Servidor: " + ack);
            RegistroMetricas.escribir("simulador_sismo.log",
                    RegistroMetricas.linea("SISMO_SOLICITADO", "servidor=" + host + ":" + puerto,
                            "idEvento=" + idEvento,
                            "duracionMs=" + duracionMs,
                            "retardoClienteMs=" + retardoClienteMs,
                            "ack=" + ack));
        } catch (Exception ex) {
            RegistroMetricas.escribir("simulador_sismo.log",
                    RegistroMetricas.linea("SISMO_SIMULADOR_ERROR", "servidor=" + host + ":" + puerto,
                            "idEvento=" + idEvento,
                            "error=" + ex.getClass().getSimpleName(), "detalle=" + ex.getMessage()));
            ex.printStackTrace();
        }
    }
}
