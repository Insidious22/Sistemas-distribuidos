package socket.conconexion.acelerografo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import telemetria.RegistroMetricas;

public class HiloDespachador implements Runnable {
    private static final ConcurrentHashMap<String, EstadisticasCliente> CLIENTES = new ConcurrentHashMap<String, EstadisticasCliente>();
    private static AdministradorRecursosSismicos recursosSismicos;

    private final Socket socket;

    public HiloDespachador(Socket socket) {
        this.socket = socket;
    }

    public static void configurarAdministradorRecursos(AdministradorRecursosSismicos administrador) {
        recursosSismicos = administrador;
    }

    @Override
    public void run() {
        String idSesion = "SIN_IDENTIFICAR-" + socket.getPort();
        String clienteRemoto = direccionRemota();
        EstadisticasCliente estadisticas = new EstadisticasCliente(idSesion, clienteRemoto);

        try (Socket cliente = socket;
                BufferedReader entrada = new BufferedReader(
                        new InputStreamReader(cliente.getInputStream(), StandardCharsets.UTF_8));
                PrintWriter salida = new PrintWriter(
                        new OutputStreamWriter(cliente.getOutputStream(), StandardCharsets.UTF_8), true)) {

            RegistroMetricas.escribir("servidor_tcp_metricas.log",
                    RegistroMetricas.linea("TCP_CONEXION_ABIERTA", "cliente=" + direccionRemota(cliente)));

            String linea;
            while ((linea = entrada.readLine()) != null) {
                if (linea.trim().isEmpty()) {
                    salida.println("ACK_ERROR:PAYLOAD_VACIO");
                    continue;
                }

                if (linea.startsWith("HELLO;")) {
                    idSesion = extraerCampo(linea, "ID_ACEL", idSesion);
                    clienteRemoto = direccionRemota(cliente);
                    final String remotoIdentificado = clienteRemoto;
                    estadisticas = CLIENTES.computeIfAbsent(idSesion,
                            id -> new EstadisticasCliente(id, remotoIdentificado));
                    salida.println("ACK_HELLO:" + idSesion + ";RECIBIDO:" + Instant.now());
                    System.out.println("Cliente TCP conectado permanentemente: " + idSesion + " desde "
                            + clienteRemoto);
                    RegistroMetricas.escribir("servidor_tcp_metricas.log",
                            RegistroMetricas.linea("TCP_CLIENTE_CONECTADO", "idAcel=" + idSesion,
                                    "cliente=" + clienteRemoto));
                    continue;
                }

                if (linea.startsWith("METRICA_TCP;")) {
                    String idAcel = extraerCampo(linea, "ID_ACEL", idSesion);
                    double rttMs = Double.parseDouble(extraerCampo(linea, "RTT_MS", "0"));
                    double redEstimadaMs = Double.parseDouble(extraerCampo(linea, "RED_ESTIMADA_MS", "0"));
                    double idaEstimadaMs = Double.parseDouble(extraerCampo(linea, "IDA_ESTIMADA_MS", "0"));
                    double vueltaEstimadaMs = Double.parseDouble(extraerCampo(linea, "VUELTA_ESTIMADA_MS", "0"));
                    estadisticas = CLIENTES.computeIfAbsent(idAcel,
                            id -> new EstadisticasCliente(id, direccionRemota(cliente)));
                    estadisticas.registrarMetricaCliente(rttMs, redEstimadaMs, idaEstimadaMs, vueltaEstimadaMs);
                    RegistroMetricas.escribir("servidor_tcp_metricas.log",
                            RegistroMetricas.linea("TCP_RTT_REPORTADO_POR_CLIENTE", "idAcel=" + idAcel,
                                    "seq=" + extraerCampo(linea, "SEQ", "-"), "rttMs=" + RegistroMetricas.ms(rttMs),
                                    "redEstimadaMs=" + RegistroMetricas.ms(redEstimadaMs),
                                    "idaEstimadaMs=" + RegistroMetricas.ms(idaEstimadaMs),
                                    "vueltaEstimadaMs=" + RegistroMetricas.ms(vueltaEstimadaMs),
                                    estadisticas.resumen()));
                    RegistroMetricas.escribir("servidor_tcp_resumen.log",
                            RegistroMetricas.evento("METRICA", "PROMEDIO_CLIENTE", idAcel,
                                    unirCampos("seq=" + extraerCampo(linea, "SEQ", "-"),
                                            "rttActualMs=" + RegistroMetricas.ms(rttMs),
                                            estadisticas.resumenCampos())));
                    continue;
                }

                procesarLectura(cliente, salida, linea, estadisticas);
            }
            System.out.println("Cliente desconectado: " + idSesion + " desde " + clienteRemoto);
            RegistroMetricas.escribir("servidor_tcp_metricas.log",
                    RegistroMetricas.linea("TCP_CLIENTE_DESCONECTADO", "idAcel=" + idSesion,
                            "cliente=" + clienteRemoto, "modo=FIN_DE_STREAM"));
        } catch (SocketException ex) {
            System.out.println("Cliente desconectado abruptamente: " + idSesion + " desde " + clienteRemoto);
            RegistroMetricas.escribir("servidor_tcp_metricas.log",
                    RegistroMetricas.linea("TCP_CLIENTE_DESCONECTADO", "idAcel=" + idSesion,
                            "cliente=" + clienteRemoto, "modo=ABRUPTO",
                            "detalle=" + ex.getMessage()));
        } catch (IOException ex) {
            System.out.println("Error de E/S con cliente " + idSesion + ". El servidor sigue activo.");
            RegistroMetricas.escribir("servidor_tcp_metricas.log",
                    RegistroMetricas.linea("TCP_CLIENTE_IO_ERROR", "idAcel=" + idSesion,
                            "cliente=" + clienteRemoto, "error=" + ex.getClass().getSimpleName(),
                            "detalle=" + ex.getMessage()));
        } catch (Exception ex) {
            System.out.println("No se pudo procesar una lectura TCP de " + idSesion + ". El servidor sigue activo.");
            RegistroMetricas.escribir("servidor_tcp_metricas.log",
                    RegistroMetricas.linea("TCP_SERVIDOR_ERROR", "idAcel=" + idSesion,
                            "cliente=" + clienteRemoto,
                            "error=" + ex.getClass().getSimpleName(), "detalle=" + ex.getMessage()));
            ex.printStackTrace();
        } finally {
            RegistroMetricas.escribir("servidor_tcp_metricas.log",
                    RegistroMetricas.linea("TCP_CONEXION_CERRADA", "idAcel=" + idSesion,
                            "cliente=" + clienteRemoto, estadisticas.resumen()));
        }
    }

    private static void procesarLectura(Socket cliente, PrintWriter salida, String payload,
            EstadisticasCliente estadisticas) {
        Instant servidorRecibido = Instant.now();
        long tiempoInicial = System.nanoTime();
        LecturaTriaxial lectura = LecturaTriaxial.desdePayload(payload);
        obtenerAdministradorRecursos().procesarLecturaEstacion(lectura.getIdAcel(), payload);
        long tiempoFinal = System.nanoTime();
        double procesamientoMs = (tiempoFinal - tiempoInicial) / 1_000_000.0;
        String seq = extraerCampo(payload, "SEQ", "-");
        Instant servidorAck = Instant.now();

        estadisticas.registrarLectura(procesamientoMs);
        System.out.println("TCP recibido desde " + direccionRemota(cliente) + " -> " + lectura + " SEQ=" + seq);
        salida.println("ACK_TCP;ID_ACEL:" + lectura.getIdAcel() + ";SEQ:" + seq + ";SERVIDOR_RECIBIDO:"
                + servidorRecibido + ";SERVIDOR_ACK:" + servidorAck + ";SERVIDOR_PROC_MS:"
                + RegistroMetricas.ms(procesamientoMs));
        RegistroMetricas.escribir("servidor_tcp_metricas.log",
                RegistroMetricas.linea("TCP_LECTURA_RECIBIDA", "idAcel=" + lectura.getIdAcel(), "seq=" + seq,
                        "cliente=" + direccionRemota(cliente), "servidorRecibido=" + servidorRecibido,
                        "servidorAck=" + servidorAck,
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

    private static synchronized AdministradorRecursosSismicos obtenerAdministradorRecursos() {
        if (recursosSismicos == null) {
            recursosSismicos = new AdministradorRecursosSismicos();
        }
        return recursosSismicos;
    }

    private static String[] unirCampos(String campo1, String campo2, String[] otrosCampos) {
        String[] campos = new String[otrosCampos.length + 2];
        campos[0] = campo1;
        campos[1] = campo2;
        System.arraycopy(otrosCampos, 0, campos, 2, otrosCampos.length);
        return campos;
    }

    private String direccionRemota() {
        return direccionRemota(socket);
    }

    private static String direccionRemota(Socket cliente) {
        try {
            return String.valueOf(cliente.getRemoteSocketAddress());
        } catch (Exception ex) {
            return "desconocido";
        }
    }
}
