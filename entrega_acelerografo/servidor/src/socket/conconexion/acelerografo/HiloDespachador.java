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
    private static final String LOG_SISMO = "sismo_distribuido.log";
    private static final ConcurrentHashMap<String, EstadisticasCliente> CLIENTES = new ConcurrentHashMap<String, EstadisticasCliente>();
    private static AdministradorRecursosSismicos recursosSismicos;
    private static String idServidor = "SERVIDOR-ACEL";
    private static int prioridadServidor = 1;
    private static int puertoServidor = 7500;

    private final Socket socket;

    public HiloDespachador(Socket socket) {
        this.socket = socket;
    }

    public static void configurarAdministradorRecursos(AdministradorRecursosSismicos administrador) {
        recursosSismicos = administrador;
    }

    public static void configurarNodoServidor(String idNodo, int prioridad, int puerto) {
        idServidor = idNodo;
        prioridadServidor = prioridad;
        puertoServidor = puerto;
        RegistroMetricas.escribir(LOG_SISMO,
                RegistroMetricas.linea("NODO_SERVIDOR_CONFIGURADO", "idServidor=" + idServidor,
                        "prioridad=" + prioridadServidor, "puerto=" + puertoServidor));
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

            RegistroMetricas.escribir(LOG_SISMO,
                    RegistroMetricas.linea("TCP_CONEXION_ABIERTA", "cliente=" + direccionRemota(cliente)));

            String linea;
            while ((linea = entrada.readLine()) != null) {
                if (linea.trim().isEmpty()) {
                    salida.println("ACK_ERROR:PAYLOAD_VACIO");
                    continue;
                }

                if (linea.startsWith("ELECCION_PING;")) {
                    String idAcel = extraerCampo(linea, "ID_ACEL", idSesion);
                    String ronda = extraerCampo(linea, "RONDA", "-");
                    salida.println("ELECCION_ACK;ID_SERV:" + idServidor + ";PRIORIDAD:" + prioridadServidor
                            + ";PUERTO:" + puertoServidor + ";ESTADO:VIVO;RONDA:" + ronda + ";RECIBIDO:"
                            + Instant.now());
                    RegistroMetricas.escribir(LOG_SISMO,
                            RegistroMetricas.linea("ELECCION_PING_RECIBIDO", "idAcel=" + idAcel,
                                    "ronda=" + ronda, "cliente=" + direccionRemota(cliente),
                                    "respuesta=VIVO", "idServidor=" + idServidor,
                                    "prioridad=" + prioridadServidor));
                    continue;
                }

                if (linea.startsWith("LOG_CLIENTE;")) {
                    procesarLogCliente(linea, direccionRemota(cliente), salida);
                    continue;
                }

                if (linea.startsWith("SUSCRIBIR_ALERTAS;")) {
                    idSesion = extraerCampo(linea, "ID_ACEL", idSesion);
                    clienteRemoto = direccionRemota(cliente);
                    GestorAlertasSismo.registrar(idSesion, salida, clienteRemoto);
                    salida.println("ACK_SUSCRIPCION_ALERTAS;ID_ACEL:" + idSesion + ";RECIBIDO:" + Instant.now());
                    System.out.println("Cliente suscrito a alertas de sismo: " + idSesion + " desde "
                            + clienteRemoto);
                    continue;
                }

                if (linea.startsWith("SIMULAR_SISMO;")) {
                    String idEvento = extraerCampo(linea, "ID_EVENTO", "SISMO-" + System.currentTimeMillis());
                    long duracionMs = Long.parseLong(extraerCampo(linea, "DURACION_MS", "10000"));
                    long retardoClienteMs = Long.parseLong(extraerCampo(linea, "RETARDO_CLIENTE_MS",
                            Long.toString(duracionMs)));
                    GestorAlertasSismo.simularSismo(idEvento, duracionMs, retardoClienteMs);
                    salida.println("ACK_SIMULACION_SISMO;ID_EVENTO:" + idEvento + ";DURACION_MS:" + duracionMs
                            + ";RETARDO_CLIENTE_MS:" + retardoClienteMs + ";RECIBIDO:" + Instant.now());
                    continue;
                }

                if (linea.startsWith("CONSULTAR_SISMO;")) {
                    idSesion = extraerCampo(linea, "ID_ACEL", idSesion);
                    clienteRemoto = direccionRemota(cliente);
                    int prioridadCliente = Integer.parseInt(extraerCampo(linea, "PRIORIDAD", "9"));
                    long retardoAtencionMs = Long.parseLong(extraerCampo(linea, "RETARDO_ATENCION_MS", "5000"));
                    GestorAlertasSismo.consultarMovimiento(idSesion, prioridadCliente, clienteRemoto, salida,
                            retardoAtencionMs);
                    continue;
                }

                if (linea.startsWith("HELLO;")) {
                    idSesion = extraerCampo(linea, "ID_ACEL", idSesion);
                    clienteRemoto = direccionRemota(cliente);
                    final String remotoIdentificado = clienteRemoto;
                    estadisticas = CLIENTES.computeIfAbsent(idSesion,
                            id -> new EstadisticasCliente(id, remotoIdentificado));
                    salida.println("ACK_HELLO:" + idSesion + ";ID_SERV:" + idServidor + ";PRIORIDAD:"
                            + prioridadServidor + ";RECIBIDO:" + Instant.now());
                    System.out.println("Cliente TCP conectado permanentemente: " + idSesion + " desde "
                            + clienteRemoto);
                    RegistroMetricas.escribir(LOG_SISMO,
                            RegistroMetricas.linea("TCP_CLIENTE_CONECTADO", "idAcel=" + idSesion,
                                    "cliente=" + clienteRemoto, "idServidor=" + idServidor,
                                    "prioridad=" + prioridadServidor));
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
                    RegistroMetricas.escribir(LOG_SISMO,
                            RegistroMetricas.linea("TCP_RTT_REPORTADO_POR_CLIENTE", "idAcel=" + idAcel,
                                    "seq=" + extraerCampo(linea, "SEQ", "-"), "rttMs=" + RegistroMetricas.ms(rttMs),
                                    "redEstimadaMs=" + RegistroMetricas.ms(redEstimadaMs),
                                    "idaEstimadaMs=" + RegistroMetricas.ms(idaEstimadaMs),
                                    "vueltaEstimadaMs=" + RegistroMetricas.ms(vueltaEstimadaMs),
                                    estadisticas.resumen()));
                    RegistroMetricas.escribir(LOG_SISMO,
                            RegistroMetricas.evento("METRICA", "PROMEDIO_CLIENTE", idAcel,
                                    unirCampos("seq=" + extraerCampo(linea, "SEQ", "-"),
                                            "rttActualMs=" + RegistroMetricas.ms(rttMs),
                                            estadisticas.resumenCampos())));
                    continue;
                }

                procesarLectura(cliente, salida, linea, estadisticas);
            }
            System.out.println("Cliente desconectado: " + idSesion + " desde " + clienteRemoto);
            RegistroMetricas.escribir(LOG_SISMO,
                    RegistroMetricas.linea("TCP_CLIENTE_DESCONECTADO", "idAcel=" + idSesion,
                            "cliente=" + clienteRemoto, "modo=FIN_DE_STREAM"));
        } catch (SocketException ex) {
            System.out.println("Cliente desconectado abruptamente: " + idSesion + " desde " + clienteRemoto);
            RegistroMetricas.escribir(LOG_SISMO,
                    RegistroMetricas.linea("TCP_CLIENTE_DESCONECTADO", "idAcel=" + idSesion,
                            "cliente=" + clienteRemoto, "modo=ABRUPTO",
                            "detalle=" + ex.getMessage()));
        } catch (IOException ex) {
            System.out.println("Error de E/S con cliente " + idSesion + ". El servidor sigue activo.");
            RegistroMetricas.escribir(LOG_SISMO,
                    RegistroMetricas.linea("TCP_CLIENTE_IO_ERROR", "idAcel=" + idSesion,
                            "cliente=" + clienteRemoto, "error=" + ex.getClass().getSimpleName(),
                            "detalle=" + ex.getMessage()));
        } catch (Exception ex) {
            System.out.println("No se pudo procesar una lectura TCP de " + idSesion + ". El servidor sigue activo.");
            RegistroMetricas.escribir(LOG_SISMO,
                    RegistroMetricas.linea("TCP_SERVIDOR_ERROR", "idAcel=" + idSesion,
                            "cliente=" + clienteRemoto,
                            "error=" + ex.getClass().getSimpleName(), "detalle=" + ex.getMessage()));
            ex.printStackTrace();
        } finally {
            GestorAlertasSismo.desregistrar(idSesion);
            RegistroMetricas.escribir(LOG_SISMO,
                    RegistroMetricas.linea("TCP_CONEXION_CERRADA", "idAcel=" + idSesion,
                            "cliente=" + clienteRemoto, estadisticas.resumen()));
        }
    }

    private static void procesarLectura(Socket cliente, PrintWriter salida, String payload,
            EstadisticasCliente estadisticas) {
        Instant servidorRecibido = Instant.now();
        long tiempoInicial = System.nanoTime();
        LecturaTriaxial lectura = LecturaTriaxial.desdePayload(payload);
        AdministradorRecursosSismicos.ResultadoAdmision admision =
                obtenerAdministradorRecursos().procesarLecturaEstacion(lectura.getIdAcel(), payload);
        long tiempoFinal = System.nanoTime();
        double procesamientoMs = (tiempoFinal - tiempoInicial) / 1_000_000.0;
        String seq = extraerCampo(payload, "SEQ", "-");
        Instant servidorAck = Instant.now();

        if (admision.isBloqueado()) {
            System.out.println("TCP bloqueado por semaforo desde " + direccionRemota(cliente) + " -> "
                    + lectura + " SEQ=" + seq);
            salida.println("ACK_BLOQUEADO;ID_ACEL:" + lectura.getIdAcel() + ";SEQ:" + seq
                    + ";MOTIVO:SEMAFORO_SATURADO;SERVIDOR_RECIBIDO:" + servidorRecibido
                    + ";SERVIDOR_ACK:" + servidorAck + ";SERVIDOR_PROC_MS:" + RegistroMetricas.ms(procesamientoMs)
                    + ";ESPERA_SEMAFORO_MS:" + RegistroMetricas.ms(admision.getEsperaMs())
                    + ";PERMISOS_DISPONIBLES:" + admision.getPermisosDisponibles());
            RegistroMetricas.escribir(LOG_SISMO,
                    RegistroMetricas.linea("TCP_LECTURA_BLOQUEADA", "idAcel=" + lectura.getIdAcel(),
                            "seq=" + seq,
                            "cliente=" + direccionRemota(cliente),
                            "servidorRecibido=" + servidorRecibido,
                            "servidorAck=" + servidorAck,
                            "procesamientoMs=" + RegistroMetricas.ms(procesamientoMs),
                            "esperaSemaforoMs=" + RegistroMetricas.ms(admision.getEsperaMs()),
                            "permisosDisponibles=" + admision.getPermisosDisponibles(),
                            "prioridadCliente=" + extraerCampo(payload, "PRIORIDAD_CLIENTE", "-"),
                            "distanciaM=" + extraerCampo(payload, "DISTANCIA_M", "-"),
                            "movimiento=" + extraerCampo(payload, "MOVIMIENTO", "-"),
                            "intensidad=" + extraerCampo(payload, "INTENSIDAD", "-"),
                            "origen=" + extraerCampo(payload, "ORIGEN", "-"),
                            "payload=" + payload,
                            estadisticas.resumen()));
            return;
        }

        estadisticas.registrarLectura(procesamientoMs);
        System.out.println("TCP recibido desde " + direccionRemota(cliente) + " -> " + lectura + " SEQ=" + seq);
        salida.println("ACK_TCP;ID_ACEL:" + lectura.getIdAcel() + ";SEQ:" + seq + ";SERVIDOR_RECIBIDO:"
                + servidorRecibido + ";SERVIDOR_ACK:" + servidorAck + ";SERVIDOR_PROC_MS:"
                + RegistroMetricas.ms(procesamientoMs) + ";ESPERA_SEMAFORO_MS:"
                + RegistroMetricas.ms(admision.getEsperaMs()) + ";PERMISOS_DISPONIBLES:"
                + admision.getPermisosDisponibles());
        RegistroMetricas.escribir(LOG_SISMO,
                RegistroMetricas.linea("TCP_LECTURA_RECIBIDA", "idAcel=" + lectura.getIdAcel(), "seq=" + seq,
                        "cliente=" + direccionRemota(cliente), "servidorRecibido=" + servidorRecibido,
                        "servidorAck=" + servidorAck,
                        "procesamientoMs=" + RegistroMetricas.ms(procesamientoMs),
                        "esperaSemaforoMs=" + RegistroMetricas.ms(admision.getEsperaMs()),
                        "permisosDisponibles=" + admision.getPermisosDisponibles(),
                        "prioridadCliente=" + extraerCampo(payload, "PRIORIDAD_CLIENTE", "-"),
                        "distanciaM=" + extraerCampo(payload, "DISTANCIA_M", "-"),
                        "movimiento=" + extraerCampo(payload, "MOVIMIENTO", "-"),
                        "intensidad=" + extraerCampo(payload, "INTENSIDAD", "-"),
                        "origen=" + extraerCampo(payload, "ORIGEN", "-"),
                        "payload=" + payload,
                        estadisticas.resumen()));
    }

    private static void procesarLogCliente(String linea, String clienteRemoto, PrintWriter salida) {
        String idAcel = extraerCampo(linea, "ID_ACEL", "SIN_ID");
        String evento = extraerCampo(linea, "EVENTO", "SIN_EVENTO");
        String seq = extraerCampo(linea, "SEQ", "-");
        RegistroMetricas.escribir(LOG_SISMO,
                RegistroMetricas.linea("MONITOR_CLIENTE", "idAcel=" + idAcel,
                        "eventoCliente=" + evento,
                        "seq=" + seq,
                        "clienteRemoto=" + clienteRemoto,
                        "coordinador=" + extraerCampo(linea, "COORDINADOR", "-"),
                        "idServidor=" + extraerCampo(linea, "ID_SERV", "-"),
                        "prioridad=" + extraerCampo(linea, "PRIORIDAD", "-"),
                        "prioridadCliente=" + extraerCampo(linea, "PRIORIDADCLIENTE", "-"),
                        "distanciaM=" + extraerCampo(linea, "DISTANCIAM", "-"),
                        "movimiento=" + extraerCampo(linea, "MOVIMIENTO", "-"),
                        "intensidad=" + extraerCampo(linea, "INTENSIDAD", "-"),
                        "motivo=" + extraerCampo(linea, "MOTIVO", "-"),
                        "rttMs=" + extraerCampo(linea, "RTT_MS", "-"),
                        "redMs=" + extraerCampo(linea, "RED_MS", "-"),
                        "pendientes=" + extraerCampo(linea, "PENDIENTES", "-"),
                        "origen=" + extraerCampo(linea, "ORIGEN", "-"),
                        "detalle=" + extraerCampo(linea, "DETALLE", "-"),
                        "clienteTs=" + extraerCampo(linea, "CLIENTE_TS", "-")));
        salida.println("ACK_LOG;ID_ACEL:" + idAcel + ";EVENTO:" + evento + ";RECIBIDO:" + Instant.now());
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
