package socket.conconexion.acelerografo;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import telemetria.RegistroMetricas;

public class HiloDespachador implements Runnable {
    private static final ConcurrentHashMap<String, EstadisticasCliente> CLIENTES = new ConcurrentHashMap<String, EstadisticasCliente>();

    private final Socket socket;

    public HiloDespachador(Socket socket) {
        this.socket = socket;
    }

    @Override
    public void run() {
        String idSesion = "SIN_IDENTIFICAR-" + socket.getPort();
        EstadisticasCliente estadisticas = new EstadisticasCliente(idSesion, String.valueOf(socket.getRemoteSocketAddress()));

        try (Socket cliente = socket;
                BufferedReader entrada = new BufferedReader(
                        new InputStreamReader(cliente.getInputStream(), StandardCharsets.UTF_8));
                PrintWriter salida = new PrintWriter(
                        new OutputStreamWriter(cliente.getOutputStream(), StandardCharsets.UTF_8), true)) {

            RegistroMetricas.escribir("servidor_tcp_metricas.log",
                    RegistroMetricas.linea("TCP_CONEXION_ABIERTA", "cliente=" + cliente.getRemoteSocketAddress()));

            String linea;
            while ((linea = entrada.readLine()) != null) {
                if (linea.trim().isEmpty()) {
                    salida.println("ACK_ERROR:PAYLOAD_VACIO");
                    continue;
                }

                if (linea.startsWith("HELLO;")) {
                    idSesion = extraerCampo(linea, "ID_ACEL", idSesion);
                    estadisticas = CLIENTES.computeIfAbsent(idSesion,
                            id -> new EstadisticasCliente(id, String.valueOf(cliente.getRemoteSocketAddress())));
                    salida.println("ACK_HELLO:" + idSesion + ";RECIBIDO:" + Instant.now());
                    System.out.println("Cliente TCP conectado permanentemente: " + idSesion + " desde "
                            + cliente.getRemoteSocketAddress());
                    RegistroMetricas.escribir("servidor_tcp_metricas.log",
                            RegistroMetricas.linea("TCP_CLIENTE_CONECTADO", "idAcel=" + idSesion,
                                    "cliente=" + cliente.getRemoteSocketAddress()));
                    continue;
                }

                if (linea.startsWith("METRICA_TCP;")) {
                    String idAcel = extraerCampo(linea, "ID_ACEL", idSesion);
                    double rttMs = Double.parseDouble(extraerCampo(linea, "RTT_MS", "0"));
                    estadisticas = CLIENTES.computeIfAbsent(idAcel,
                            id -> new EstadisticasCliente(id, String.valueOf(cliente.getRemoteSocketAddress())));
                    estadisticas.registrarRttCliente(rttMs);
                    RegistroMetricas.escribir("servidor_tcp_metricas.log",
                            RegistroMetricas.linea("TCP_RTT_REPORTADO_POR_CLIENTE", "idAcel=" + idAcel,
                                    "seq=" + extraerCampo(linea, "SEQ", "-"), "rttMs=" + RegistroMetricas.ms(rttMs),
                                    estadisticas.resumen()));
                    continue;
                }

                procesarLectura(cliente, salida, linea, estadisticas);
            }
        } catch (Exception ex) {
            System.out.println("No se pudo procesar una lectura TCP.");
            RegistroMetricas.escribir("servidor_tcp_metricas.log",
                    RegistroMetricas.linea("TCP_SERVIDOR_ERROR", "cliente=" + socket.getRemoteSocketAddress(),
                            "error=" + ex.getClass().getSimpleName(), "detalle=" + ex.getMessage()));
            ex.printStackTrace();
        } finally {
            RegistroMetricas.escribir("servidor_tcp_metricas.log",
                    RegistroMetricas.linea("TCP_CONEXION_CERRADA", "idAcel=" + idSesion,
                            "cliente=" + socket.getRemoteSocketAddress(), estadisticas.resumen()));
        }
    }

    private static void procesarLectura(Socket cliente, PrintWriter salida, String payload,
            EstadisticasCliente estadisticas) {
        long tiempoInicial = System.nanoTime();
        LecturaTriaxial lectura = LecturaTriaxial.desdePayload(payload);
        long tiempoFinal = System.nanoTime();
        double procesamientoMs = (tiempoFinal - tiempoInicial) / 1_000_000.0;
        String seq = extraerCampo(payload, "SEQ", "-");

        estadisticas.registrarLectura(procesamientoMs);
        System.out.println("TCP recibido desde " + cliente.getRemoteSocketAddress() + " -> " + lectura + " SEQ=" + seq);
        salida.println("ACK_TCP:" + lectura.getIdAcel() + ";SEQ:" + seq + ";RECIBIDO:" + Instant.now());
        RegistroMetricas.escribir("servidor_tcp_metricas.log",
                RegistroMetricas.linea("TCP_LECTURA_RECIBIDA", "idAcel=" + lectura.getIdAcel(), "seq=" + seq,
                        "cliente=" + cliente.getRemoteSocketAddress(),
                        "procesamientoMs=" + RegistroMetricas.ms(procesamientoMs), "payload=" + payload,
                        estadisticas.resumen()));
    }

    private static String extraerCampo(String linea, String campo, String defecto) {
        Map<String, String> campos = new HashMap<String, String>();
        for (String parte : linea.split(";")) {
            int separador = parte.indexOf(':');
            if (separador > 0) {
                campos.put(parte.substring(0, separador), parte.substring(separador + 1));
            }
        }
        String valor = campos.get(campo);
        return valor == null || valor.trim().isEmpty() ? defecto : valor.trim();
    }
}
