package socket.conconexion.acelerografo;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import telemetria.RegistroMetricas;

public class EstacionAcelTCP {
    private static final int PUERTO_DEFECTO = 7500;
    private static final long INTERVALO_DEFECTO_MS = 60000L;
    private static final long INTERVALO_INTERACTIVO_MS = 1000L;
    private static final String ID_ACEL_DEFECTO = "EC-UCON01";
    private static final int REPETICIONES_PERMANENTE = 0;
    private static final int TIMEOUT_CONEXION_MS = 2000;
    private static final int TIMEOUT_ACK_MS = 5000;
    private static final int TIMEOUT_MONITOR_MS = 400;
    private static final int PAUSA_REELECCION_MS = 1000;
    private static final long REVISION_PRIORIDAD_MS = 5000L;

    public static void main(String[] args) {
        try {
            boolean modoInteractivo = args.length == 0;
            String servidoresTexto = args.length >= 1
                    ? args[0]
                    : leerTexto("Servidores TCP separados por coma (ip:puerto,ip:puerto)", "");
            if (servidoresTexto.trim().isEmpty()) {
                imprimirUso();
                return;
            }

            int puerto = args.length >= 2 ? Integer.parseInt(args[1]) : leerEntero("Puerto TCP", PUERTO_DEFECTO);
            String idAcel = args.length >= 3 ? args[2] : leerTexto("ID del acelerografo", ID_ACEL_DEFECTO);
            long intervaloMs = args.length >= 4
                    ? Long.parseLong(args[3])
                    : leerEntero("Intervalo entre envios en ms", modoInteractivo
                            ? (int) INTERVALO_INTERACTIVO_MS
                            : (int) INTERVALO_DEFECTO_MS);
            int repeticiones = args.length >= 5 ? Integer.parseInt(args[4]) : REPETICIONES_PERMANENTE;
            int duracionSegundos = args.length >= 6 ? Integer.parseInt(args[5]) : 0;
            long finNs = duracionSegundos > 0
                    ? System.nanoTime() + duracionSegundos * 1_000_000_000L
                    : Long.MAX_VALUE;

            List<NodoServidor> servidores = parsearServidores(servidoresTexto, puerto);
            NodoServidor coordinador = elegirCoordinador(servidores, idAcel, "INICIO", 0);
            long proximaRevisionPrioridadNs = siguienteRevisionPrioridadNs();
            ArrayDeque<LecturaPendiente> pendientes = new ArrayDeque<LecturaPendiente>();

            int indice = 0;
            while (puedeGenerar(indice, repeticiones, finNs) || !pendientes.isEmpty()) {
                if (coordinador == null) {
                    if (puedeGenerar(indice, repeticiones, finNs)) {
                        LecturaPendiente lectura = crearLecturaPendiente(idAcel, indice);
                        pendientes.add(lectura);
                        indice++;
                        registrarLecturaEncolada(lectura, "SIN_COORDINADOR", pendientes.size());
                        publicarMonitorGlobal(servidores, idAcel, "COLA_LOCAL_ENCOLADO", lectura.secuencia,
                                coordinador, "motivo=SIN_COORDINADOR", "pendientes=" + pendientes.size(),
                                "origen=COLA_LOCAL", "detalle=lectura_guardada_sin_servidores");
                        System.out.println("Sin coordinador. Lectura " + lectura.secuencia
                                + " guardada en cola local. Pendientes=" + pendientes.size());
                        esperarSiguienteEnvio(intervaloMs, indice, repeticiones);
                    } else {
                        System.out.println("No hay servidor disponible. Pendientes por sincronizar="
                                + pendientes.size());
                        dormir(PAUSA_REELECCION_MS);
                    }
                    coordinador = elegirCoordinador(servidores, idAcel, "SIN_COORDINADOR", indice + 1);
                    proximaRevisionPrioridadNs = siguienteRevisionPrioridadNs();
                    continue;
                }

                try (ConexionServidor conexion = conectar(coordinador, idAcel)) {
                    System.out.println("Cliente TCP permanente conectado a coordinador " + coordinador.etiqueta()
                            + ". Use Ctrl + C para detener.");
                    if (repeticiones != REPETICIONES_PERMANENTE || duracionSegundos > 0) {
                        System.out.println("Modo finito: el cliente cerrara al completar la cantidad o duracion indicada.");
                    }

                    while (puedeGenerar(indice, repeticiones, finNs) || !pendientes.isEmpty()) {
                        boolean sincronizandoPendiente = !pendientes.isEmpty();
                        LecturaPendiente lectura = sincronizandoPendiente
                                ? pendientes.peek()
                                : crearLecturaPendiente(idAcel, indice);
                        boolean enviado = enviarLectura(conexion, coordinador, idAcel, lectura, servidores);
                        if (!enviado) {
                            if (!sincronizandoPendiente) {
                                pendientes.add(lectura);
                                indice++;
                                registrarLecturaEncolada(lectura, "FALLO_ENVIO", pendientes.size());
                                publicarMonitorGlobal(servidores, idAcel, "COLA_LOCAL_ENCOLADO", lectura.secuencia,
                                        coordinador, "motivo=FALLO_ENVIO", "pendientes=" + pendientes.size(),
                                        "origen=COLA_LOCAL", "detalle=lectura_guardada_por_fallo_envio");
                            }
                            coordinador = elegirCoordinador(servidores, idAcel, "FALLO_ENVIO", indice + 1);
                            break;
                        }
                        if (sincronizandoPendiente) {
                            pendientes.remove();
                            registrarLecturaSincronizada(lectura, coordinador, pendientes.size());
                            publicarMonitorGlobal(servidores, idAcel, "COLA_LOCAL_SINCRONIZADO",
                                    lectura.secuencia, coordinador, "pendientes=" + pendientes.size(),
                                    "origen=COLA_LOCAL", "detalle=lectura_pendiente_enviada_al_coordinador");
                        } else {
                            indice++;
                        }
                        if (System.nanoTime() >= proximaRevisionPrioridadNs) {
                            NodoServidor preferente = elegirCoordinador(servidores, idAcel, "REVISION_PRIORIDAD",
                                    indice);
                            proximaRevisionPrioridadNs = siguienteRevisionPrioridadNs();
                            if (debeMigrarPorPrioridad(coordinador, preferente)) {
                                registrarMigracionPorRecuperacion(coordinador, preferente, idAcel, indice);
                                publicarMonitorGlobal(servidores, idAcel, "MIGRACION_COORDINADOR", indice,
                                        preferente, "motivo=RECUPERACION_MAYOR_PRIORIDAD",
                                        "detalle=de_" + coordinador.idEfectivo() + "_a_"
                                                + preferente.idEfectivo());
                                coordinador = preferente;
                                break;
                            }
                        }
                        if (!sincronizandoPendiente) {
                            esperarSiguienteEnvio(intervaloMs, indice, repeticiones);
                        }
                    }
                } catch (Exception ex) {
                    registrarFalloCoordinador(coordinador, idAcel, "CONEXION", indice + 1, ex);
                    coordinador = elegirCoordinador(servidores, idAcel, "FALLO_CONEXION", indice + 1);
                    proximaRevisionPrioridadNs = siguienteRevisionPrioridadNs();
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private static ConexionServidor conectar(NodoServidor servidor, String idAcel) throws IOException {
        Socket socket = new Socket();
        socket.connect(new InetSocketAddress(servidor.host, servidor.puerto), TIMEOUT_CONEXION_MS);
        socket.setSoTimeout(TIMEOUT_ACK_MS);
        BufferedReader entrada = new BufferedReader(
                new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
        PrintWriter salida = new PrintWriter(
                new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8), true);

        salida.println("HELLO;ID_ACEL:" + idAcel);
        String ackHello = entrada.readLine();
        if (ackHello == null) {
            cerrarSilencioso(socket);
            throw new IOException("HELLO sin respuesta");
        }

        servidor.idReportado = extraerCampo(ackHello, "ID_SERV", servidor.nombre);
        servidor.prioridadReportada = parsearEntero(extraerCampo(ackHello, "PRIORIDAD",
                Integer.toString(servidor.prioridadConfigurada)), servidor.prioridadConfigurada);
        System.out.println("Servidor: " + ackHello);
        RegistroMetricas.escribir("cliente_eleccion.log",
                RegistroMetricas.linea("COORDINADOR_CONECTADO", "idAcel=" + idAcel,
                        "servidor=" + servidor.etiqueta(),
                        "idServidor=" + servidor.idEfectivo(),
                        "prioridad=" + servidor.prioridadEfectiva()));

        return new ConexionServidor(socket, entrada, salida);
    }

    private static boolean enviarLectura(ConexionServidor conexion, NodoServidor servidor, String idAcel,
            LecturaPendiente lectura, List<NodoServidor> servidores) {
        try {
            Instant clienteEnvio = Instant.now();
            long tiempoInicial = System.nanoTime();
            conexion.salida.println(lectura.payload);
            if (conexion.salida.checkError()) {
                throw new IOException("No se pudo escribir en el socket");
            }

            String ack = conexion.entrada.readLine();
            if (ack == null) {
                throw new IOException("ACK nulo: el servidor cerro la conexion");
            }

            long tiempoFinal = System.nanoTime();
            Instant clienteAck = Instant.now();
            double tTxMs = (tiempoFinal - tiempoInicial) / 1_000_000.0;
            double servidorProcMs = Double.parseDouble(extraerCampo(ack, "SERVIDOR_PROC_MS", "0"));
            double redEstimadaMs = Math.max(0.0, tTxMs - servidorProcMs);
            double idaEstimadaMs = redEstimadaMs / 2.0;
            double vueltaEstimadaMs = redEstimadaMs / 2.0;

            System.out.println("ACK recibido desde " + servidor.etiqueta() + ": " + ack);
            System.out.printf(Locale.US, "[METRICA TCP] Registro enviado en: %.4f ms.%n", tTxMs);
            System.out.printf(Locale.US,
                    "[DESGLOSE TCP] servidor=%s | servidorProc=%.4f ms | redEstimada=%.4f ms | idaEstimada=%.4f ms | vueltaEstimada=%.4f ms%n",
                    servidor.idEfectivo(), servidorProcMs, redEstimadaMs, idaEstimadaMs, vueltaEstimadaMs);
            conexion.salida.println("METRICA_TCP;ID_ACEL:" + idAcel + ";SEQ:" + lectura.secuencia + ";RTT_MS:"
                    + RegistroMetricas.ms(tTxMs) + ";SERVIDOR_PROC_MS:" + RegistroMetricas.ms(servidorProcMs)
                    + ";RED_ESTIMADA_MS:" + RegistroMetricas.ms(redEstimadaMs) + ";IDA_ESTIMADA_MS:"
                    + RegistroMetricas.ms(idaEstimadaMs) + ";VUELTA_ESTIMADA_MS:"
                    + RegistroMetricas.ms(vueltaEstimadaMs));
            RegistroMetricas.escribir("cliente_tcp_metricas.log",
                    RegistroMetricas.linea("TCP_CLIENTE", "idAcel=" + idAcel, "seq=" + lectura.secuencia,
                            "coordinador=" + servidor.etiqueta(), "idServidor=" + servidor.idEfectivo(),
                            "prioridad=" + servidor.prioridadEfectiva(),
                            "origen=" + lectura.origen,
                            "generadoEn=" + lectura.generadoEn,
                            "tTxMs=" + RegistroMetricas.ms(tTxMs),
                            "servidorProcMs=" + RegistroMetricas.ms(servidorProcMs),
                            "redEstimadaMs=" + RegistroMetricas.ms(redEstimadaMs),
                            "idaEstimadaMs=" + RegistroMetricas.ms(idaEstimadaMs),
                            "vueltaEstimadaMs=" + RegistroMetricas.ms(vueltaEstimadaMs),
                            "clienteEnvio=" + clienteEnvio, "servidorRecibido="
                                    + extraerCampo(ack, "SERVIDOR_RECIBIDO", "-"),
                            "servidorAck=" + extraerCampo(ack, "SERVIDOR_ACK", "-"),
                            "clienteAck=" + clienteAck, "ack=" + ack, "payload=" + lectura.payload));
            publicarMonitorGlobal(servidores, idAcel, "LECTURA_ENVIADA", lectura.secuencia, servidor,
                    "rttMs=" + RegistroMetricas.ms(tTxMs),
                    "redMs=" + RegistroMetricas.ms(redEstimadaMs),
                    "origen=" + lectura.origen,
                    "detalle=ack_recibido");
            return true;
        } catch (Exception ex) {
            registrarFalloCoordinador(servidor, idAcel, "ENVIO", lectura.secuencia, ex);
            return false;
        }
    }

    private static LecturaPendiente crearLecturaPendiente(String idAcel, int indice) {
        double x = 0.020 + indice * 0.001;
        double y = -0.010 - indice * 0.001;
        double z = 0.980 + indice * 0.001;
        int secuencia = indice + 1;
        String payload = "SEQ:" + secuencia + ";" + LecturaTriaxial.crearPayload(idAcel, x, y, z);
        return new LecturaPendiente(secuencia, payload, Instant.now());
    }

    private static boolean puedeGenerar(int indice, int repeticiones, long finNs) {
        return (repeticiones == REPETICIONES_PERMANENTE || indice < repeticiones)
                && System.nanoTime() < finNs;
    }

    private static void registrarLecturaEncolada(LecturaPendiente lectura, String motivo, int pendientes) {
        RegistroMetricas.escribir("cliente_eleccion.log",
                RegistroMetricas.linea("COLA_LOCAL_ENCOLADO", "seq=" + lectura.secuencia,
                        "motivo=" + motivo, "pendientes=" + pendientes,
                        "generadoEn=" + lectura.generadoEn, "payload=" + lectura.payload));
    }

    private static void registrarLecturaSincronizada(LecturaPendiente lectura, NodoServidor servidor, int pendientes) {
        RegistroMetricas.escribir("cliente_eleccion.log",
                RegistroMetricas.linea("COLA_LOCAL_SINCRONIZADO", "seq=" + lectura.secuencia,
                        "coordinador=" + servidor.etiqueta(), "idServidor=" + servidor.idEfectivo(),
                        "prioridad=" + servidor.prioridadEfectiva(), "pendientes=" + pendientes,
                        "generadoEn=" + lectura.generadoEn, "sincronizadoEn=" + Instant.now()));
    }

    private static NodoServidor elegirCoordinador(List<NodoServidor> servidores, String idAcel, String motivo,
            int secuencia) {
        String ronda = motivo + "-" + System.currentTimeMillis();
        List<ResultadoEleccion> resultados = new ArrayList<ResultadoEleccion>();
        NodoServidor ganador = null;

        for (NodoServidor servidor : servidores) {
            ResultadoEleccion resultado = comprobarServidor(servidor, idAcel, ronda);
            resultados.add(resultado);
            if (resultado.vivo) {
                if (ganador == null || servidor.prioridadEfectiva() > ganador.prioridadEfectiva()) {
                    ganador = servidor;
                }
            }
        }

        RegistroMetricas.escribir("cliente_eleccion.log",
                tablaEleccion(idAcel, motivo, secuencia, ronda, resultados, ganador));

        if (ganador != null) {
            System.out.println("Coordinador elegido: " + ganador.etiqueta() + " prioridad="
                    + ganador.prioridadEfectiva());
        } else {
            System.out.println("Eleccion sin ganador: ningun servidor respondio.");
        }
        publicarMonitorGlobal(servidores, idAcel, "ELECCION_COORDINADOR", secuencia, ganador,
                "motivo=" + motivo,
                "detalle=" + (ganador == null ? "sin_ganador" : "ganador_" + ganador.idEfectivo()));
        return ganador;
    }

    private static ResultadoEleccion comprobarServidor(NodoServidor servidor, String idAcel, String ronda) {
        long inicio = System.nanoTime();
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(servidor.host, servidor.puerto), TIMEOUT_CONEXION_MS);
            socket.setSoTimeout(TIMEOUT_CONEXION_MS);
            BufferedReader entrada = new BufferedReader(
                    new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
            PrintWriter salida = new PrintWriter(
                    new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8), true);

            salida.println("ELECCION_PING;ID_ACEL:" + idAcel + ";RONDA:" + ronda + ";CLIENTE_TS:" + Instant.now());
            String respuesta = entrada.readLine();
            double rttMs = (System.nanoTime() - inicio) / 1_000_000.0;
            if (respuesta == null) {
                return ResultadoEleccion.caido(servidor, "SIN_RESPUESTA", rttMs);
            }

            servidor.idReportado = extraerCampo(respuesta, "ID_SERV", servidor.nombre);
            servidor.prioridadReportada = parsearEntero(extraerCampo(respuesta, "PRIORIDAD",
                    Integer.toString(servidor.prioridadConfigurada)), servidor.prioridadConfigurada);
            return ResultadoEleccion.vivo(servidor, rttMs, respuesta);
        } catch (SocketTimeoutException ex) {
            double rttMs = (System.nanoTime() - inicio) / 1_000_000.0;
            return ResultadoEleccion.caido(servidor, "TIMEOUT", rttMs);
        } catch (Exception ex) {
            double rttMs = (System.nanoTime() - inicio) / 1_000_000.0;
            return ResultadoEleccion.caido(servidor, ex.getClass().getSimpleName(), rttMs);
        }
    }

    private static void registrarFalloCoordinador(NodoServidor servidor, String idAcel, String fase, int secuencia,
            Exception ex) {
        String etiqueta = servidor == null ? "SIN_COORDINADOR" : servidor.etiqueta();
        System.out.println("Fallo con coordinador " + etiqueta + ". Iniciando eleccion...");
        RegistroMetricas.escribir("cliente_eleccion.log",
                RegistroMetricas.linea("FALLO_COORDINADOR", "idAcel=" + idAcel, "fase=" + fase,
                        "seq=" + secuencia, "coordinador=" + etiqueta,
                        "error=" + ex.getClass().getSimpleName(), "detalle=" + ex.getMessage()));
    }

    private static boolean debeMigrarPorPrioridad(NodoServidor actual, NodoServidor preferente) {
        if (actual == null || preferente == null) {
            return false;
        }
        return !mismoServidor(actual, preferente)
                && preferente.prioridadEfectiva() > actual.prioridadEfectiva();
    }

    private static boolean mismoServidor(NodoServidor a, NodoServidor b) {
        return a.host.equalsIgnoreCase(b.host) && a.puerto == b.puerto;
    }

    private static void registrarMigracionPorRecuperacion(NodoServidor anterior, NodoServidor nuevo, String idAcel,
            int secuencia) {
        System.out.println("Servidor de mayor prioridad recuperado. Migrando de " + anterior.etiqueta()
                + " a " + nuevo.etiqueta());
        RegistroMetricas.escribir("cliente_eleccion.log",
                RegistroMetricas.linea("MIGRACION_COORDINADOR_RECUPERADO", "idAcel=" + idAcel,
                        "seq=" + secuencia,
                        "anterior=" + anterior.etiqueta(),
                        "prioridadAnterior=" + anterior.prioridadEfectiva(),
                        "nuevo=" + nuevo.etiqueta(),
                        "prioridadNueva=" + nuevo.prioridadEfectiva(),
                        "criterio=mayor_prioridad_viva"));
    }

    private static long siguienteRevisionPrioridadNs() {
        return System.nanoTime() + REVISION_PRIORIDAD_MS * 1_000_000L;
    }

    private static void publicarMonitorGlobal(List<NodoServidor> servidores, String idAcel, String evento,
            int secuencia, NodoServidor coordinador, String... campos) {
        if (servidores == null || servidores.isEmpty()) {
            return;
        }

        StringBuilder mensaje = new StringBuilder();
        mensaje.append("LOG_CLIENTE;ID_ACEL:").append(idAcel);
        mensaje.append(";EVENTO:").append(evento);
        mensaje.append(";SEQ:").append(secuencia);
        mensaje.append(";COORDINADOR:").append(coordinador == null ? "-" : coordinador.etiqueta());
        mensaje.append(";ID_SERV:").append(coordinador == null ? "-" : coordinador.idEfectivo());
        mensaje.append(";PRIORIDAD:").append(coordinador == null ? "-" : coordinador.prioridadEfectiva());
        mensaje.append(";CLIENTE_TS:").append(Instant.now());
        for (String campo : campos) {
            agregarCampoMonitor(mensaje, campo);
        }

        for (NodoServidor servidor : servidores) {
            enviarMonitorAServidor(servidor, mensaje.toString());
        }
    }

    private static void enviarMonitorAServidor(NodoServidor servidor, String mensaje) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(servidor.host, servidor.puerto), TIMEOUT_MONITOR_MS);
            socket.setSoTimeout(TIMEOUT_MONITOR_MS);
            BufferedReader entrada = new BufferedReader(
                    new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
            PrintWriter salida = new PrintWriter(
                    new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8), true);
            salida.println(mensaje);
            entrada.readLine();
        } catch (Exception ex) {
            // El monitor global es de mejor esfuerzo: si un servidor esta caido,
            // recibira nuevos eventos cuando vuelva a estar disponible.
        }
    }

    private static void agregarCampoMonitor(StringBuilder mensaje, String campo) {
        if (campo == null || campo.trim().isEmpty()) {
            return;
        }
        int separador = campo.indexOf('=');
        if (separador <= 0) {
            mensaje.append(";DETALLE:").append(limpiarValorMonitor(campo));
            return;
        }
        String clave = claveMonitor(campo.substring(0, separador));
        String valor = limpiarValorMonitor(campo.substring(separador + 1));
        mensaje.append(';').append(clave).append(':').append(valor);
    }

    private static String claveMonitor(String clave) {
        String normalizada = clave.trim();
        if ("rttMs".equalsIgnoreCase(normalizada)) {
            return "RTT_MS";
        }
        if ("redMs".equalsIgnoreCase(normalizada)) {
            return "RED_MS";
        }
        if ("pendientes".equalsIgnoreCase(normalizada)) {
            return "PENDIENTES";
        }
        if ("motivo".equalsIgnoreCase(normalizada)) {
            return "MOTIVO";
        }
        if ("origen".equalsIgnoreCase(normalizada)) {
            return "ORIGEN";
        }
        if ("detalle".equalsIgnoreCase(normalizada)) {
            return "DETALLE";
        }
        return normalizada.toUpperCase(Locale.US);
    }

    private static String limpiarValorMonitor(String valor) {
        return valor == null ? "" : valor.replace(';', ',').replace('|', '/');
    }

    private static List<NodoServidor> parsearServidores(String texto, int puertoDefecto) {
        List<NodoServidor> servidores = new ArrayList<NodoServidor>();
        String[] partes = texto.split(",");
        int prioridad = partes.length;
        int indice = 1;
        for (String parte : partes) {
            String valor = parte.trim();
            if (valor.isEmpty()) {
                continue;
            }

            String host = valor;
            int puerto = puertoDefecto;
            int separadorPuerto = valor.lastIndexOf(':');
            if (separadorPuerto > 0 && separadorPuerto < valor.length() - 1) {
                host = valor.substring(0, separadorPuerto);
                puerto = Integer.parseInt(valor.substring(separadorPuerto + 1));
            }

            servidores.add(new NodoServidor("SERV-" + indice, host, puerto, prioridad));
            prioridad--;
            indice++;
        }
        return servidores;
    }

    private static String tablaEleccion(String idAcel, String motivo, int secuencia, String ronda,
            List<ResultadoEleccion> resultados, NodoServidor ganador) {
        StringBuilder b = new StringBuilder();
        lineaTabla(b);
        b.append("| ELECCION DE COORDINADOR TCP").append(rellenar("", 53)).append("|\n");
        lineaTabla(b);
        b.append("| idAcel=").append(rellenar(idAcel, 14));
        b.append(" motivo=").append(rellenar(motivo, 18));
        b.append(" seq=").append(rellenar(Integer.toString(secuencia), 8));
        b.append(" ronda=").append(rellenar(ronda, 23)).append("|\n");
        lineaTabla(b);
        b.append("| No | Servidor                 | Pri | Estado | RTT ms     | Detalle                 |\n");
        lineaTabla(b);
        int i = 1;
        for (ResultadoEleccion r : resultados) {
            b.append("| ").append(rellenar(Integer.toString(i), 2));
            b.append(" | ").append(rellenar(r.servidor.etiqueta(), 24));
            b.append(" | ").append(rellenar(Integer.toString(r.servidor.prioridadEfectiva()), 3));
            b.append(" | ").append(rellenar(r.vivo ? "VIVO" : "CAIDO", 6));
            b.append(" | ").append(rellenar(RegistroMetricas.ms(r.rttMs), 10));
            b.append(" | ").append(rellenar(r.detalle, 23)).append("|\n");
            i++;
        }
        lineaTabla(b);
        String ganadorTexto = ganador == null ? "SIN GANADOR" : ganador.etiqueta() + " prioridad="
                + ganador.prioridadEfectiva();
        b.append("| GANADOR: ").append(rellenar(ganadorTexto, 70)).append("|\n");
        lineaTabla(b);
        return b.toString();
    }

    private static void lineaTabla(StringBuilder b) {
        b.append("+--------------------------------------------------------------------------------+\n");
    }

    private static String rellenar(String valor, int longitud) {
        String texto = valor == null ? "" : valor;
        if (texto.length() > longitud) {
            return texto.substring(0, longitud);
        }
        StringBuilder b = new StringBuilder(texto);
        while (b.length() < longitud) {
            b.append(' ');
        }
        return b.toString();
    }

    private static void esperarSiguienteEnvio(long intervaloMs, int indice, int repeticiones) {
        if (repeticiones != REPETICIONES_PERMANENTE && indice >= repeticiones) {
            return;
        }
        dormir(intervaloMs);
    }

    private static void dormir(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }

    private static void cerrarSilencioso(Closeable closeable) {
        try {
            closeable.close();
        } catch (Exception ex) {
            // Cierre defensivo durante error de conexion.
        }
    }

    private static void imprimirUso() {
        System.out.println("Uso clasico:");
        System.out.println("  java -cp out socket.conconexion.acelerografo.EstacionAcelTCP <ipServidor> [puerto] [idAcel] [intervaloMs] [repeticiones] [duracionSegundos]");
        System.out.println("Uso redundante con eleccion:");
        System.out.println("  java -cp out socket.conconexion.acelerografo.EstacionAcelTCP <ipA:puerto,ipB:puerto,ipC:puerto> [puertoDefecto] [idAcel] [intervaloMs] [repeticiones] [duracionSegundos]");
        System.out.println("Sin argumentos, el cliente pide datos basicos y queda permanente hasta Ctrl + C.");
    }

    private static String leerTexto(String mensaje, String valorDefecto) throws Exception {
        String etiquetaDefecto = valorDefecto == null || valorDefecto.isEmpty() ? "" : " [" + valorDefecto + "]";
        System.out.print(mensaje + etiquetaDefecto + ": ");
        String valor = new BufferedReader(new InputStreamReader(System.in)).readLine();
        if (valor == null || valor.trim().isEmpty()) {
            return valorDefecto;
        }
        return valor.trim();
    }

    private static int leerEntero(String mensaje, int valorDefecto) throws Exception {
        return Integer.parseInt(leerTexto(mensaje, Integer.toString(valorDefecto)));
    }

    private static String extraerCampo(String linea, String campo, String defecto) {
        Map<String, String> campos = new HashMap<String, String>();
        if (linea == null) {
            return defecto;
        }
        for (String parte : linea.split(";")) {
            int separador = parte.indexOf(':');
            if (separador > 0) {
                campos.put(parte.substring(0, separador), parte.substring(separador + 1));
            }
        }
        String valor = campos.get(campo);
        return valor == null || valor.trim().isEmpty() ? defecto : valor.trim();
    }

    private static int parsearEntero(String texto, int defecto) {
        try {
            return Integer.parseInt(texto);
        } catch (Exception ex) {
            return defecto;
        }
    }

    private static final class LecturaPendiente {
        private final int secuencia;
        private final String payload;
        private final Instant generadoEn;
        private final String origen;

        private LecturaPendiente(int secuencia, String payload, Instant generadoEn) {
            this.secuencia = secuencia;
            this.payload = payload;
            this.generadoEn = generadoEn;
            this.origen = "COLA_LOCAL";
        }
    }

    private static final class ConexionServidor implements Closeable {
        private final Socket socket;
        private final BufferedReader entrada;
        private final PrintWriter salida;

        private ConexionServidor(Socket socket, BufferedReader entrada, PrintWriter salida) {
            this.socket = socket;
            this.entrada = entrada;
            this.salida = salida;
        }

        public void close() throws IOException {
            socket.close();
        }
    }

    private static final class NodoServidor {
        private final String nombre;
        private final String host;
        private final int puerto;
        private final int prioridadConfigurada;
        private String idReportado;
        private int prioridadReportada;

        private NodoServidor(String nombre, String host, int puerto, int prioridadConfigurada) {
            this.nombre = nombre;
            this.host = host;
            this.puerto = puerto;
            this.prioridadConfigurada = prioridadConfigurada;
            this.idReportado = nombre;
            this.prioridadReportada = prioridadConfigurada;
        }

        private String idEfectivo() {
            return idReportado == null || idReportado.trim().isEmpty() ? nombre : idReportado;
        }

        private int prioridadEfectiva() {
            return prioridadReportada > 0 ? prioridadReportada : prioridadConfigurada;
        }

        private String etiqueta() {
            return idEfectivo() + "(" + host + ":" + puerto + ")";
        }
    }

    private static final class ResultadoEleccion {
        private final NodoServidor servidor;
        private final boolean vivo;
        private final double rttMs;
        private final String detalle;

        private ResultadoEleccion(NodoServidor servidor, boolean vivo, double rttMs, String detalle) {
            this.servidor = servidor;
            this.vivo = vivo;
            this.rttMs = rttMs;
            this.detalle = detalle;
        }

        private static ResultadoEleccion vivo(NodoServidor servidor, double rttMs, String respuesta) {
            return new ResultadoEleccion(servidor, true, rttMs, extraerCampo(respuesta, "ESTADO", "VIVO"));
        }

        private static ResultadoEleccion caido(NodoServidor servidor, String detalle, double rttMs) {
            return new ResultadoEleccion(servidor, false, rttMs, detalle);
        }
    }
}
