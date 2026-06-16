package socket.conconexion.acelerografo;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import telemetria.RegistroMetricas;

public class ClienteConsultaSismoTCP {
    private static final String LOG_SISMO = "sismo_distribuido.log";

    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Uso: java -cp out socket.conconexion.acelerografo.ClienteConsultaSismoTCP <ipServidor> <puerto> <idCliente> [prioridad] [retardoAtencionMs]");
            return;
        }

        String host = args[0];
        int puerto = Integer.parseInt(args[1]);
        String idCliente = args[2];
        int prioridad = args.length >= 4 ? Integer.parseInt(args[3]) : 9;
        long retardoAtencionMs = args.length >= 5 ? Long.parseLong(args[4]) : 5000L;

        try (Socket socket = new Socket(host, puerto);
                BufferedReader entrada = new BufferedReader(
                        new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
                PrintWriter salida = new PrintWriter(
                        new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8), true)) {

            socket.setSoTimeout(30000);
            String comando = "CONSULTAR_SISMO;ID_ACEL:" + idCliente
                    + ";PRIORIDAD:" + prioridad
                    + ";RETARDO_ATENCION_MS:" + retardoAtencionMs
                    + ";CLIENTE_DETECTA:" + Instant.now();

            System.out.println("Movimiento local detectado por " + idCliente + ". Consultando al servidor...");
            RegistroMetricas.escribir(LOG_SISMO,
                    RegistroMetricas.linea("CLIENTE_DETECTA_MOVIMIENTO",
                            "idAcel=" + idCliente,
                            "prioridad=" + prioridad,
                            "servidor=" + host + ":" + puerto,
                            "retardoAtencionMs=" + retardoAtencionMs));

            salida.println(comando);

            String linea;
            while ((linea = entrada.readLine()) != null) {
                System.out.println("Servidor -> " + linea);
                RegistroMetricas.escribir(LOG_SISMO,
                        RegistroMetricas.linea(eventoDesdeRespuesta(linea),
                                "idAcel=" + idCliente,
                                "prioridad=" + prioridad,
                                "servidor=" + host + ":" + puerto,
                                "movimiento=" + extraerCampo(linea, "MOVIMIENTO", "-"),
                                "idEvento=" + extraerCampo(linea, "ID_EVENTO", "-"),
                                "bloqueadoPor=" + extraerCampo(linea, "CLIENTE_EN_ATENCION", "-"),
                                "esperaAvisoMs=" + extraerCampo(linea, "ESPERA_AVISO_MS", "-"),
                                "esperaTotalMs=" + extraerCampo(linea, "ESPERA_TOTAL_MS", "-"),
                                "xG=" + extraerCampo(linea, "X_G", "-"),
                                "yG=" + extraerCampo(linea, "Y_G", "-"),
                                "zG=" + extraerCampo(linea, "Z_G", "-"),
                                "intensidad=" + extraerCampo(linea, "INTENSIDAD", "-"),
                                "payload=" + linea));

                if (linea.startsWith("RESPUESTA_CONSULTA_SISMO;")) {
                    break;
                }
            }
        } catch (Exception ex) {
            RegistroMetricas.escribir(LOG_SISMO,
                    RegistroMetricas.linea("CLIENTE_CONSULTA_ERROR",
                            "idAcel=" + idCliente,
                            "servidor=" + host + ":" + puerto,
                            "error=" + ex.getClass().getSimpleName(),
                            "detalle=" + ex.getMessage()));
            ex.printStackTrace();
        }
    }

    private static String eventoDesdeRespuesta(String linea) {
        if (linea != null && linea.startsWith("BLOQUEADO_CONSULTA;")) {
            return "CLIENTE_RECIBE_AVISO_BLOQUEO";
        }
        if (linea != null && linea.startsWith("RESPUESTA_CONSULTA_SISMO;")) {
            return "CLIENTE_RECIBE_RESPUESTA_SISMO";
        }
        return "CLIENTE_RECIBE_MENSAJE_SERVIDOR";
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
