package socket.conconexion.acelerografo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import telemetria.RegistroMetricas;

public class HiloDespachador implements Runnable {
    private static final String LOG_SISMO = "sismo_distribuido.log";
    private static final ConcurrentHashMap<String, EstadisticasCliente> CLIENTES = new ConcurrentHashMap<String, EstadisticasCliente>();
    private static final ConcurrentHashMap<String, Long> ULTIMO_ENVIO_CLIENTE_MS = new ConcurrentHashMap<String, Long>();
    private static final ConcurrentHashMap<String, Long> ULTIMA_LLEGADA_SERVIDOR_MS = new ConcurrentHashMap<String, Long>();
    private static final List<NodoReplica> REPLICAS = Collections.synchronizedList(new ArrayList<NodoReplica>());
    private static final List<String> REPLICAS_PENDIENTES = Collections.synchronizedList(new ArrayList<String>());
    private static final Set<String> REPLICAS_RECIBIDAS = Collections.synchronizedSet(new HashSet<String>());
    private static volatile boolean replicadorIniciado;
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

    public static void configurarReplicas(String replicasTexto) {
        REPLICAS.clear();
        if (replicasTexto != null && !replicasTexto.trim().isEmpty()) {
            String[] partes = replicasTexto.split(",");
            for (String parte : partes) {
                NodoReplica replica = NodoReplica.desdeTexto(parte.trim());
                if (replica != null) {
                    REPLICAS.add(replica);
                }
            }
        }
        iniciarReplicadorSiHaceFalta();
    }

    private static synchronized void iniciarReplicadorSiHaceFalta() {
        if (replicadorIniciado) {
            return;
        }
        replicadorIniciado = true;
        Thread hilo = new Thread(new Runnable() {
            public void run() {
                while (true) {
                    sincronizarReplicasPendientes();
                    dormir(3000L);
                }
            }
        }, "replicador-servidor");
        hilo.setDaemon(true);
        hilo.start();
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

                if (linea.startsWith("REPLICA_LECTURA;")) {
                    procesarReplicaServidor(linea, salida);
                    continue;
                }

                if (linea.startsWith("SUSCRIBIR_ALERTAS;")) {
                    idSesion = extraerCampo(linea, "ID_ACEL", idSesion);
                    clienteRemoto = direccionRemota(cliente);
                    GestorAlertasSismo.registrar(idSesion, salida, clienteRemoto);
                    salida.println("ACK_SUSCRIPCION_ALERTAS;ID_ACEL:" + idSesion + ";RECIBIDO:" + Instant.now());
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

                if (linea.startsWith("ESTADO_SISMO;")) {
                    String idAcel = extraerCampo(linea, "ID_ACEL", idSesion);
                    int prioridadCliente = Integer.parseInt(extraerCampo(linea, "PRIORIDAD", "9"));
                    double distanciaM = Double.parseDouble(extraerCampo(linea, "DISTANCIA_M", "99"));
                    salida.println(GestorAlertasSismo.estadoSismoParaLectura(idAcel, prioridadCliente, distanciaM));
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
            RegistroMetricas.escribir(LOG_SISMO,
                    RegistroMetricas.linea("TCP_CLIENTE_DESCONECTADO", "idAcel=" + idSesion,
                            "cliente=" + clienteRemoto, "modo=FIN_DE_STREAM"));
        } catch (SocketException ex) {
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
            imprimirResumenTerminal("BLOQUEADO", lectura, payload, seq, servidorRecibido, procesamientoMs,
                    admision.getEsperaMs());
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
            RegistroMetricas.escribir(LOG_SISMO,
                    RegistroMetricas.linea("TCP_CONEXION_RECHAZADA_POR_OCUPACION",
                            "idAcel=" + lectura.getIdAcel(),
                            "seq=" + seq,
                            "cliente=" + direccionRemota(cliente),
                            "accion=cliente_debe_encolar_localmente_y_reconectar"));
            try {
                cliente.close();
            } catch (IOException ex) {
                RegistroMetricas.escribir(LOG_SISMO,
                        RegistroMetricas.linea("TCP_CIERRE_TRAS_BLOQUEO_ERROR",
                                "idAcel=" + lectura.getIdAcel(),
                                "seq=" + seq,
                                "error=" + ex.getClass().getSimpleName(),
                                "detalle=" + ex.getMessage()));
            }
            return;
        }

        estadisticas.registrarLectura(procesamientoMs);
        imprimirResumenTerminal("RECIBIDO", lectura, payload, seq, servidorRecibido, procesamientoMs,
                admision.getEsperaMs());
        encolarReplicaServidor(payload);
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

    private static void procesarReplicaServidor(String linea, PrintWriter salida) {
        String origenServidor = extraerCampo(linea, "ID_SERV_ORIGEN", "SERVIDOR-DESCONOCIDO");
        String payloadB64 = extraerCampo(linea, "PAYLOAD_B64", "");
        try {
            String payload = new String(Base64.getDecoder().decode(payloadB64), StandardCharsets.UTF_8);
            String clave = claveReplica(origenServidor, payload);
            if (REPLICAS_RECIBIDAS.add(clave)) {
                obtenerAdministradorRecursos().registrarReplicaServidor(origenServidor, payload);
                System.out.println("REPLICA  | desde " + origenServidor + " | "
                        + extraerCampo(payload, "ID_ACEL", "-") + " seq=" + extraerCampo(payload, "SEQ", "-")
                        + " sincronizada");
            }
            salida.println("ACK_REPLICA_OK;ID_SERV:" + idServidor + ";ORIGEN:" + origenServidor
                    + ";RECIBIDO:" + Instant.now());
        } catch (Exception ex) {
            salida.println("ACK_REPLICA_ERROR;ID_SERV:" + idServidor + ";ERROR:"
                    + ex.getClass().getSimpleName());
            RegistroMetricas.escribir(LOG_SISMO,
                    RegistroMetricas.linea("REPLICA_SERVIDOR_ERROR",
                            "origenServidor=" + origenServidor,
                            "error=" + ex.getClass().getSimpleName(),
                            "detalle=" + ex.getMessage()));
        }
    }

    private static void encolarReplicaServidor(String payload) {
        if (REPLICAS.isEmpty()) {
            return;
        }
        synchronized (REPLICAS_PENDIENTES) {
            REPLICAS_PENDIENTES.add(payload);
        }
        sincronizarReplicasPendientes();
    }

    private static void sincronizarReplicasPendientes() {
        if (REPLICAS.isEmpty() || REPLICAS_PENDIENTES.isEmpty()) {
            return;
        }

        List<String> copia;
        synchronized (REPLICAS_PENDIENTES) {
            copia = new ArrayList<String>(REPLICAS_PENDIENTES);
        }

        for (String payload : copia) {
            boolean replicado = false;
            synchronized (REPLICAS) {
                for (NodoReplica replica : REPLICAS) {
                    if (enviarReplica(replica, payload)) {
                        replicado = true;
                        break;
                    }
                }
            }
            if (replicado) {
                synchronized (REPLICAS_PENDIENTES) {
                    REPLICAS_PENDIENTES.remove(payload);
                }
            }
        }
    }

    private static boolean enviarReplica(NodoReplica replica, String payload) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(replica.host, replica.puerto), 800);
            socket.setSoTimeout(1200);
            BufferedReader entrada = new BufferedReader(
                    new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
            PrintWriter salida = new PrintWriter(
                    new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8), true);
            String payloadB64 = Base64.getEncoder().encodeToString(payload.getBytes(StandardCharsets.UTF_8));
            salida.println("REPLICA_LECTURA;ID_SERV_ORIGEN:" + idServidor + ";PAYLOAD_B64:" + payloadB64
                    + ";ENVIADO:" + Instant.now());
            String ack = entrada.readLine();
            boolean ok = ack != null && ack.startsWith("ACK_REPLICA_OK;");
            if (ok) {
                RegistroMetricas.escribir(LOG_SISMO,
                        RegistroMetricas.linea("REPLICA_SERVIDOR_ENVIADA",
                                "destino=" + replica.etiqueta(),
                                "idAcel=" + extraerCampo(payload, "ID_ACEL", "-"),
                                "seq=" + extraerCampo(payload, "SEQ", "-"),
                                "pendientes=" + REPLICAS_PENDIENTES.size()));
            }
            return ok;
        } catch (Exception ex) {
            return false;
        }
    }

    private static String claveReplica(String origenServidor, String payload) {
        return origenServidor + "|" + extraerCampo(payload, "ID_ACEL", "-") + "|"
                + extraerCampo(payload, "SEQ", "-") + "|" + extraerCampo(payload, "TIMESTAMP", "-");
    }

    private static void imprimirResumenTerminal(String estado, LecturaTriaxial lectura, String payload, String seq,
            Instant servidorRecibido, double procesamientoMs, double esperaMs) {
        String idAcel = lectura.getIdAcel();
        String movimiento = extraerCampo(payload, "MOVIMIENTO", "-");
        if (!"RECIBIDO".equals(estado) || !"SI".equals(movimiento)) {
            return;
        }
        String intensidad = extraerCampo(payload, "INTENSIDAD", "-");
        String magnitud = extraerCampo(payload, "MAGNITUD", "-");
        String direccion = extraerCampo(payload, "DIRECCION", "-");
        String ejeX = extraerCampo(payload, "EJE_X", "-");
        String ejeY = extraerCampo(payload, "EJE_Y", "-");
        String ejeZ = extraerCampo(payload, "EJE_Z", "-");
        String origen = extraerCampo(payload, "ORIGEN", "-");
        String pendientes = extraerCampo(payload, "PENDIENTES_CLIENTE", "0");
        String intervaloEnvio = calcularIntervaloEnvio(idAcel, extraerCampo(payload, "TIMESTAMP", ""));
        String intervaloLlegada = calcularIntervaloLlegada(idAcel, servidorRecibido);
        String hora = servidorRecibido.toString();
        if (hora.length() >= 19) {
            hora = hora.substring(11, 19);
        }

        System.out.printf(Locale.US,
                "%s | SISMO RECIBIDO | %-8s seq=%s mag=%s dir=%s int=%s x=%s y=%s z=%s origen=%s pend=%s envioCada=%s llegaCada=%s proc=%s ms%n",
                hora,
                idAcel,
                seq,
                magnitud,
                direccion,
                intensidad,
                ejeX,
                ejeY,
                ejeZ,
                origen,
                pendientes,
                intervaloEnvio,
                intervaloLlegada,
                RegistroMetricas.ms(procesamientoMs));
    }

    private static String calcularIntervaloEnvio(String idAcel, String timestampTexto) {
        try {
            long actualMs = Instant.parse(timestampTexto).toEpochMilli();
            Long anteriorMs = ULTIMO_ENVIO_CLIENTE_MS.get(idAcel);
            if (anteriorMs == null || actualMs <= anteriorMs.longValue()) {
                ULTIMO_ENVIO_CLIENTE_MS.put(idAcel, actualMs);
                return "-";
            }
            ULTIMO_ENVIO_CLIENTE_MS.put(idAcel, actualMs);
            return formatoIntervalo(actualMs - anteriorMs.longValue());
        } catch (Exception ex) {
            return "-";
        }
    }

    private static String calcularIntervaloLlegada(String idAcel, Instant llegada) {
        long actualMs = llegada.toEpochMilli();
        Long anteriorMs = ULTIMA_LLEGADA_SERVIDOR_MS.put(idAcel, actualMs);
        if (anteriorMs == null || actualMs <= anteriorMs.longValue()) {
            return "-";
        }
        return formatoIntervalo(actualMs - anteriorMs.longValue());
    }

    private static String formatoIntervalo(long deltaMs) {
        if (deltaMs >= 1000L) {
            return String.format(Locale.US, "%.1fs", deltaMs / 1000.0);
        }
        return deltaMs + "ms";
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

    private static void dormir(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
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

    private static final class NodoReplica {
        private final String host;
        private final int puerto;

        private NodoReplica(String host, int puerto) {
            this.host = host;
            this.puerto = puerto;
        }

        private static NodoReplica desdeTexto(String texto) {
            if (texto == null || texto.isEmpty()) {
                return null;
            }
            int separador = texto.lastIndexOf(':');
            if (separador <= 0 || separador >= texto.length() - 1) {
                return null;
            }
            return new NodoReplica(texto.substring(0, separador),
                    Integer.parseInt(texto.substring(separador + 1)));
        }

        private String etiqueta() {
            return host + ":" + puerto;
        }
    }
}
