package socket.conconexion.acelerografo;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import telemetria.RegistroMetricas;

public class ClienteAlertaSismoTCP {
    private static final int PUERTO_DEFECTO = 7500;

    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Uso: java -cp out socket.conconexion.acelerografo.ClienteAlertaSismoTCP <ipServidor> <puerto> <idCliente>");
            return;
        }

        String host = args[0];
        int puerto = args.length >= 2 ? Integer.parseInt(args[1]) : PUERTO_DEFECTO;
        String idCliente = args[2];

        try (Socket socket = new Socket(host, puerto);
                BufferedReader entrada = new BufferedReader(
                        new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
                PrintWriter salida = new PrintWriter(
                        new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8), true)) {

            salida.println("SUSCRIBIR_ALERTAS;ID_ACEL:" + idCliente);
            String ack = entrada.readLine();
            System.out.println("Servidor: " + ack);
            RegistroMetricas.escribir("cliente_alertas_sismo.log",
                    RegistroMetricas.linea("CLIENTE_ALERTA_CONECTADO", "idAcel=" + idCliente,
                            "servidor=" + host + ":" + puerto, "ack=" + ack));

            System.out.println("Esperando alertas de sismo. Use Ctrl + C para detener.");
            String linea;
            while ((linea = entrada.readLine()) != null) {
                System.out.println("ALERTA RECIBIDA -> " + linea);
                RegistroMetricas.escribir("cliente_alertas_sismo.log",
                        RegistroMetricas.linea("ALERTA_SISMO_RECIBIDA", "idAcel=" + idCliente,
                                "servidor=" + host + ":" + puerto,
                                "idEvento=" + extraerCampo(linea, "ID_EVENTO", "-"),
                                "duracionMs=" + extraerCampo(linea, "DURACION_MS", "-"),
                                "retardoClienteMs=" + extraerCampo(linea, "RETARDO_CLIENTE_MS", "-"),
                                "servidorEnvio=" + extraerCampo(linea, "SERVIDOR_ENVIO", "-"),
                                "payload=" + linea));
            }
        } catch (Exception ex) {
            RegistroMetricas.escribir("cliente_alertas_sismo.log",
                    RegistroMetricas.linea("CLIENTE_ALERTA_ERROR", "idAcel=" + idCliente,
                            "servidor=" + host + ":" + puerto,
                            "error=" + ex.getClass().getSimpleName(), "detalle=" + ex.getMessage()));
            ex.printStackTrace();
        }
    }

    private static String extraerCampo(String linea, String campo, String defecto) {
        if (linea == null) {
            return defecto;
        }
        for (String parte : linea.split(";")) {
            int separador = parte.indexOf(':');
            if (separador > 0 && parte.substring(0, separador).equals(campo)) {
                return parte.substring(separador + 1);
            }
        }
        return defecto;
    }
}
