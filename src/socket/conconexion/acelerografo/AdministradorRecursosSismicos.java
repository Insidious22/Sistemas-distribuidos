package socket.conconexion.acelerografo;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import telemetria.RegistroMetricas;

public class AdministradorRecursosSismicos {
    private static final String LOG_SISMO = "sismo_distribuido.log";
    private static final int PERMISOS_CONCURRENTES = 3;
    private static final long RETARDO_SIMULADO_DEFECTO_MS = 500L;
    private static final long AVISO_BLOQUEO_MS = 1000L;
    private static final long VENTANA_INTERBLOQUEO_MS = 500L;
    private static final long ESPERA_INTERBLOQUEO_MS = 1200L;
    private static final String RECURSO_CANAL = "CANAL_ENVIO";
    private static final String RECURSO_BITACORA = "BITACORA_SISMICA";

    private final Semaphore semaforoAdmision;
    private final Semaphore recursoCanalEnvio = new Semaphore(1, true);
    private final Semaphore recursoBitacoraSismica = new Semaphore(1, true);
    private final Object monitorInterbloqueo = new Object();
    private final long retardoSimuladoMs;
    private final File archivoBitacora = new File("bitacora_sismica.txt");
    private String duenioCanal = "-";
    private String duenioBitacora = "-";
    private String esperaCanal = "-";
    private String esperaBitacora = "-";
    private long ciclosDetectados;
    private boolean cicloInterbloqueoActivo;

    public AdministradorRecursosSismicos() {
        this(PERMISOS_CONCURRENTES);
    }

    public AdministradorRecursosSismicos(int permisosConcurrentes) {
        this(permisosConcurrentes, RETARDO_SIMULADO_DEFECTO_MS);
    }

    public AdministradorRecursosSismicos(int permisosConcurrentes, long retardoSimuladoMs) {
        semaforoAdmision = new Semaphore(permisosConcurrentes, true);
        this.retardoSimuladoMs = retardoSimuladoMs;
        RegistroMetricas.escribir(LOG_SISMO,
                RegistroMetricas.evento("CONFIG", "SEMAFORO_INICIALIZADO", "SERVIDOR",
                        "permisosMax=" + permisosConcurrentes,
                        "fairness=true", "retardoSimuladoMs=" + retardoSimuladoMs,
                        "avisoBloqueoMs=" + AVISO_BLOQUEO_MS,
                        "interbloqueo=activo",
                        "recursos=" + RECURSO_CANAL + "," + RECURSO_BITACORA));
    }

    public ResultadoAdmision procesarLecturaEstacion(String idEstacion, String tramaDatos) {
        boolean permisoAdquirido = false;
        long esperaInicialNs = System.nanoTime();

        try {
            System.out.println("[Control] Estacion " + idEstacion + " esperando permiso del semaforo. Permisos disponibles: "
                    + semaforoAdmision.availablePermits());
            RegistroMetricas.escribir(LOG_SISMO,
                    RegistroMetricas.evento("SEMAFORO", "ESPERA_COLA", idEstacion,
                            "dispAntes=" + semaforoAdmision.availablePermits(),
                            "hilo=" + Thread.currentThread().getName()));

            if (!semaforoAdmision.tryAcquire()) {
                Thread.sleep(AVISO_BLOQUEO_MS);
                double esperaBloqueoMs = (System.nanoTime() - esperaInicialNs) / 1_000_000.0;
                System.out.println("[Bloqueo] Estacion " + idEstacion
                        + " no obtuvo permiso. Debe guardar lectura en cola local.");
                RegistroMetricas.escribir(LOG_SISMO,
                        RegistroMetricas.evento("SEMAFORO", "LECTURA_BLOQUEADA", idEstacion,
                                "esperaAvisoMs=" + RegistroMetricas.ms(esperaBloqueoMs),
                                "dispDespues=" + semaforoAdmision.availablePermits(),
                                "motivo=permisos_agotados",
                                "hilo=" + Thread.currentThread().getName(),
                                "trama=" + tramaDatos));
                return ResultadoAdmision.bloqueado(esperaBloqueoMs, semaforoAdmision.availablePermits());
            }

            permisoAdquirido = true;
            long esperaFinalNs = System.nanoTime();
            double esperaMs = (esperaFinalNs - esperaInicialNs) / 1_000_000.0;
            long timestampProcesamiento = System.currentTimeMillis();

            System.out.println("[Admision] Permiso concedido a " + idEstacion + ". Espera: "
                    + RegistroMetricas.ms(esperaMs) + " ms");
            RegistroMetricas.escribir(LOG_SISMO,
                    RegistroMetricas.evento("SEMAFORO", "PERMISO_OTORGADO", idEstacion,
                            "esperaMs=" + RegistroMetricas.ms(esperaMs),
                            "dispDespues=" + semaforoAdmision.availablePermits(),
                            "hilo=" + Thread.currentThread().getName()));

            simularInterbloqueoSiAplica(idEstacion, tramaDatos);
            guardarEnBitacora(idEstacion, tramaDatos, timestampProcesamiento);
            Thread.sleep(retardoSimuladoMs);
            return ResultadoAdmision.admitido(esperaMs, semaforoAdmision.availablePermits());
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            RegistroMetricas.escribir(LOG_SISMO,
                    RegistroMetricas.evento("ERROR", "INTERRUPCION_SEMAFORO", idEstacion,
                            "detalle=" + ex.getMessage()));
            return ResultadoAdmision.error(ex.getMessage(), semaforoAdmision.availablePermits());
        } finally {
            if (permisoAdquirido) {
                semaforoAdmision.release();
                System.out.println("[Liberacion] " + idEstacion + " libero el permiso del semaforo.");
                RegistroMetricas.escribir(LOG_SISMO,
                        RegistroMetricas.evento("SEMAFORO", "PERMISO_LIBERADO", idEstacion,
                                "dispDespues=" + semaforoAdmision.availablePermits(),
                                "hilo=" + Thread.currentThread().getName()));
            }
        }
    }

    private void simularInterbloqueoSiAplica(String idEstacion, String tramaDatos) throws InterruptedException {
        if (!"SI".equalsIgnoreCase(extraerCampo(tramaDatos, "MOVIMIENTO", "NO"))) {
            return;
        }

        int prioridadCliente = parsearEntero(extraerCampo(tramaDatos, "PRIORIDAD_CLIENTE", "9"), 9);
        boolean canalPrimero = prioridadCliente % 2 != 0;
        String primerRecurso = canalPrimero ? RECURSO_CANAL : RECURSO_BITACORA;
        String segundoRecurso = canalPrimero ? RECURSO_BITACORA : RECURSO_CANAL;

        RegistroMetricas.escribir(LOG_SISMO,
                RegistroMetricas.evento("INTERBLOQUEO", "INTENTO", idEstacion,
                        "prioridadCliente=" + prioridadCliente,
                        "orden=" + primerRecurso + "->" + segundoRecurso,
                        "movimiento=SI",
                        "hilo=" + Thread.currentThread().getName()));

        tomarRecurso(primerRecurso, idEstacion, "PRIMER_RECURSO");
        try {
            esperarOtroRecursoOTimeout();
            registrarEspera(idEstacion, segundoRecurso, primerRecurso);
            detectarYRegistrarCiclo(idEstacion, primerRecurso, segundoRecurso);

            if (!semaforoRecurso(segundoRecurso).tryAcquire(ESPERA_INTERBLOQUEO_MS, TimeUnit.MILLISECONDS)) {
                detectarYRegistrarCiclo(idEstacion, primerRecurso, segundoRecurso);
                resolverComoVictima(idEstacion, primerRecurso, segundoRecurso);
                reintentarEnOrdenSeguro(idEstacion);
                return;
            }

            limpiarEspera(idEstacion);
            asignarDuenio(segundoRecurso, idEstacion);
            RegistroMetricas.escribir(LOG_SISMO,
                    RegistroMetricas.evento("INTERBLOQUEO", "RECURSO_TOMADO", idEstacion,
                            "recurso=" + segundoRecurso,
                            "fase=SEGUNDO_RECURSO",
                            "resultado=sin_ciclo_activo"));
            Thread.sleep(100L);
            liberarRecurso(segundoRecurso, idEstacion, "SEGUNDO_RECURSO_COMPLETADO");
        } finally {
            limpiarEspera(idEstacion);
            liberarRecurso(primerRecurso, idEstacion, "PRIMER_RECURSO_COMPLETADO");
        }
    }

    private void tomarRecurso(String recurso, String idEstacion, String fase) throws InterruptedException {
        semaforoRecurso(recurso).acquire();
        asignarDuenio(recurso, idEstacion);
        RegistroMetricas.escribir(LOG_SISMO,
                RegistroMetricas.evento("INTERBLOQUEO", "RECURSO_TOMADO", idEstacion,
                        "recurso=" + recurso,
                        "fase=" + fase,
                        "hilo=" + Thread.currentThread().getName()));
    }

    private void registrarEspera(String idEstacion, String recursoEsperado, String recursoRetenido) {
        synchronized (monitorInterbloqueo) {
            if (RECURSO_CANAL.equals(recursoEsperado)) {
                esperaCanal = idEstacion;
            } else {
                esperaBitacora = idEstacion;
            }
        }
        RegistroMetricas.escribir(LOG_SISMO,
                RegistroMetricas.evento("INTERBLOQUEO", "ESPERA_RECURSO", idEstacion,
                        "recursoRetenido=" + recursoRetenido,
                        "recursoEsperado=" + recursoEsperado,
                        "duenioRecursoEsperado=" + duenioDe(recursoEsperado),
                        "timeoutMs=" + ESPERA_INTERBLOQUEO_MS));
    }

    private void detectarYRegistrarCiclo(String idEstacion, String recursoRetenido, String recursoEsperado) {
        String otroCliente;
        String recursoEsperadoPorOtro;
        boolean ciclo;
        synchronized (monitorInterbloqueo) {
            otroCliente = duenioDeSinBloqueo(recursoEsperado);
            recursoEsperadoPorOtro = recursoEsperadoPorSinBloqueo(otroCliente);
            ciclo = otroCliente != null
                    && !"-".equals(otroCliente)
                    && !otroCliente.equals(idEstacion)
                    && recursoRetenido.equals(recursoEsperadoPorOtro)
                    && idEstacion.equals(duenioDeSinBloqueo(recursoRetenido))
                    && !cicloInterbloqueoActivo;
            if (ciclo) {
                ciclosDetectados++;
                cicloInterbloqueoActivo = true;
            }
        }

        if (ciclo) {
            RegistroMetricas.escribir(LOG_SISMO,
                    RegistroMetricas.evento("INTERBLOQUEO", "INTERBLOQUEO_DETECTADO", idEstacion,
                            "ciclo=" + idEstacion + "->" + recursoEsperado + "->" + otroCliente
                                    + "->" + recursoRetenido + "->" + idEstacion,
                            "recursoRetenido=" + recursoRetenido,
                            "recursoEsperado=" + recursoEsperado,
                            "otroCliente=" + otroCliente,
                            "otroEspera=" + recursoEsperadoPorOtro,
                            "ciclosDetectados=" + ciclosDetectados));
        }
    }

    private void resolverComoVictima(String idEstacion, String recursoRetenido, String recursoEsperado) {
        boolean registrarResolucion;
        synchronized (monitorInterbloqueo) {
            registrarResolucion = cicloInterbloqueoActivo;
            cicloInterbloqueoActivo = false;
        }
        if (registrarResolucion) {
            RegistroMetricas.escribir(LOG_SISMO,
                    RegistroMetricas.evento("INTERBLOQUEO", "INTERBLOQUEO_RESUELTO", idEstacion,
                            "victima=" + idEstacion,
                            "accion=libera_recurso_retenido_y_reintenta_en_orden_seguro",
                            "recursoLiberado=" + recursoRetenido,
                            "recursoEsperado=" + recursoEsperado));
        }
        limpiarEspera(idEstacion);
        liberarRecurso(recursoRetenido, idEstacion, "VICTIMA_LIBERA_PARA_ROMPER_CICLO");
    }

    private void reintentarEnOrdenSeguro(String idEstacion) throws InterruptedException {
        RegistroMetricas.escribir(LOG_SISMO,
                RegistroMetricas.evento("INTERBLOQUEO", "REINTENTO_SEGURO_INICIO", idEstacion,
                        "ordenSeguro=" + RECURSO_CANAL + "->" + RECURSO_BITACORA));
        tomarRecurso(RECURSO_CANAL, idEstacion, "REINTENTO_CANAL");
        try {
            tomarRecurso(RECURSO_BITACORA, idEstacion, "REINTENTO_BITACORA");
            try {
                RegistroMetricas.escribir(LOG_SISMO,
                        RegistroMetricas.evento("INTERBLOQUEO", "REINTENTO_SEGURO_OK", idEstacion,
                                "resultado=sin_interbloqueo",
                                "ordenSeguro=" + RECURSO_CANAL + "->" + RECURSO_BITACORA));
            } finally {
                liberarRecurso(RECURSO_BITACORA, idEstacion, "REINTENTO_COMPLETADO");
            }
        } finally {
            liberarRecurso(RECURSO_CANAL, idEstacion, "REINTENTO_COMPLETADO");
        }
    }

    private void esperarOtroRecursoOTimeout() throws InterruptedException {
        long fin = System.currentTimeMillis() + VENTANA_INTERBLOQUEO_MS;
        while (System.currentTimeMillis() < fin) {
            synchronized (monitorInterbloqueo) {
                if (!"-".equals(duenioCanal) && !"-".equals(duenioBitacora)) {
                    return;
                }
            }
            Thread.sleep(25L);
        }
    }

    private void liberarRecurso(String recurso, String idEstacion, String motivo) {
        boolean debeLiberar = false;
        synchronized (monitorInterbloqueo) {
            if (RECURSO_CANAL.equals(recurso) && idEstacion.equals(duenioCanal)) {
                duenioCanal = "-";
                debeLiberar = true;
            } else if (RECURSO_BITACORA.equals(recurso) && idEstacion.equals(duenioBitacora)) {
                duenioBitacora = "-";
                debeLiberar = true;
            }
        }
        if (debeLiberar) {
            semaforoRecurso(recurso).release();
            RegistroMetricas.escribir(LOG_SISMO,
                    RegistroMetricas.evento("INTERBLOQUEO", "RECURSO_LIBERADO", idEstacion,
                            "recurso=" + recurso,
                            "motivo=" + motivo));
        }
    }

    private Semaphore semaforoRecurso(String recurso) {
        return RECURSO_CANAL.equals(recurso) ? recursoCanalEnvio : recursoBitacoraSismica;
    }

    private void asignarDuenio(String recurso, String idEstacion) {
        synchronized (monitorInterbloqueo) {
            if (RECURSO_CANAL.equals(recurso)) {
                duenioCanal = idEstacion;
            } else {
                duenioBitacora = idEstacion;
            }
        }
    }

    private String duenioDe(String recurso) {
        synchronized (monitorInterbloqueo) {
            return duenioDeSinBloqueo(recurso);
        }
    }

    private String duenioDeSinBloqueo(String recurso) {
        return RECURSO_CANAL.equals(recurso) ? duenioCanal : duenioBitacora;
    }

    private String recursoEsperadoPorSinBloqueo(String idEstacion) {
        if (idEstacion == null || "-".equals(idEstacion)) {
            return "-";
        }
        if (idEstacion.equals(esperaCanal)) {
            return RECURSO_CANAL;
        }
        if (idEstacion.equals(esperaBitacora)) {
            return RECURSO_BITACORA;
        }
        return "-";
    }

    private void limpiarEspera(String idEstacion) {
        synchronized (monitorInterbloqueo) {
            if (idEstacion.equals(esperaCanal)) {
                esperaCanal = "-";
            }
            if (idEstacion.equals(esperaBitacora)) {
                esperaBitacora = "-";
            }
        }
    }

    private synchronized void guardarEnBitacora(String idEstacion, String tramaDatos, long timestampProcesamiento) {
        RegistroMetricas.escribir(LOG_SISMO,
                RegistroMetricas.evento("CRITICA", "ENTRA_BITACORA", idEstacion,
                        "monitor=synchronized", "archivo=" + archivoBitacora.getName(),
                        "hilo=" + Thread.currentThread().getName()));

        try (FileWriter fw = new FileWriter(archivoBitacora, true);
                BufferedWriter bw = new BufferedWriter(fw);
                PrintWriter out = new PrintWriter(bw)) {
            out.println("TS_SERV:" + timestampProcesamiento + ";" + tramaDatos);
            RegistroMetricas.escribir(LOG_SISMO,
                    RegistroMetricas.evento("CRITICA", "ESCRITURA_OK", idEstacion,
                            "archivo=" + archivoBitacora.getName()));
        } catch (IOException ex) {
            RegistroMetricas.escribir(LOG_SISMO,
                    RegistroMetricas.evento("ERROR", "ERROR_IO_BITACORA", idEstacion,
                            "error=" + ex.getClass().getSimpleName(), "detalle=" + ex.getMessage()));
        } finally {
            RegistroMetricas.escribir(LOG_SISMO,
                    RegistroMetricas.evento("CRITICA", "SALE_BITACORA", idEstacion,
                            "monitor=synchronized", "hilo=" + Thread.currentThread().getName()));
        }
    }

    private static String extraerCampo(String trama, String campo, String defecto) {
        if (trama == null) {
            return defecto;
        }
        for (String parte : trama.split(";")) {
            int separador = parte.indexOf(':');
            if (separador > 0 && campo.equals(parte.substring(0, separador))) {
                String valor = parte.substring(separador + 1).trim();
                return valor.isEmpty() ? defecto : valor;
            }
        }
        return defecto;
    }

    private static int parsearEntero(String texto, int defecto) {
        try {
            return Integer.parseInt(texto);
        } catch (Exception ex) {
            return defecto;
        }
    }

    public static final class ResultadoAdmision {
        private final boolean admitido;
        private final boolean bloqueado;
        private final double esperaMs;
        private final int permisosDisponibles;
        private final String detalle;

        private ResultadoAdmision(boolean admitido, boolean bloqueado, double esperaMs, int permisosDisponibles,
                String detalle) {
            this.admitido = admitido;
            this.bloqueado = bloqueado;
            this.esperaMs = esperaMs;
            this.permisosDisponibles = permisosDisponibles;
            this.detalle = detalle;
        }

        public static ResultadoAdmision admitido(double esperaMs, int permisosDisponibles) {
            return new ResultadoAdmision(true, false, esperaMs, permisosDisponibles, "ADMITIDO");
        }

        public static ResultadoAdmision bloqueado(double esperaMs, int permisosDisponibles) {
            return new ResultadoAdmision(false, true, esperaMs, permisosDisponibles, "BLOQUEADO_POR_SEMAFORO");
        }

        public static ResultadoAdmision error(String detalle, int permisosDisponibles) {
            return new ResultadoAdmision(false, false, 0.0, permisosDisponibles, detalle);
        }

        public boolean isAdmitido() {
            return admitido;
        }

        public boolean isBloqueado() {
            return bloqueado;
        }

        public double getEsperaMs() {
            return esperaMs;
        }

        public int getPermisosDisponibles() {
            return permisosDisponibles;
        }

        public String getDetalle() {
            return detalle;
        }
    }
}
