package socket.conconexion.acelerografo;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.Semaphore;
import telemetria.RegistroMetricas;

public class AdministradorRecursosSismicos {
    private static final int PERMISOS_CONCURRENTES = 3;
    private static final long RETARDO_SIMULADO_DEFECTO_MS = 500L;
    private static final long AVISO_BLOQUEO_MS = 1000L;

    private final Semaphore semaforoAdmision;
    private final long retardoSimuladoMs;
    private final File archivoBitacora = new File("bitacora_sismica.txt");

    public AdministradorRecursosSismicos() {
        this(PERMISOS_CONCURRENTES);
    }

    public AdministradorRecursosSismicos(int permisosConcurrentes) {
        this(permisosConcurrentes, RETARDO_SIMULADO_DEFECTO_MS);
    }

    public AdministradorRecursosSismicos(int permisosConcurrentes, long retardoSimuladoMs) {
        semaforoAdmision = new Semaphore(permisosConcurrentes, true);
        this.retardoSimuladoMs = retardoSimuladoMs;
        RegistroMetricas.escribir("concurrencia_sismica.log",
                RegistroMetricas.evento("CONFIG", "SEMAFORO_INICIALIZADO", "SERVIDOR",
                        "permisosMax=" + permisosConcurrentes,
                        "fairness=true", "retardoSimuladoMs=" + retardoSimuladoMs,
                        "avisoBloqueoMs=" + AVISO_BLOQUEO_MS));
    }

    public ResultadoAdmision procesarLecturaEstacion(String idEstacion, String tramaDatos) {
        boolean permisoAdquirido = false;
        long esperaInicialNs = System.nanoTime();

        try {
            System.out.println("[Control] Estacion " + idEstacion + " esperando permiso del semaforo. Permisos disponibles: "
                    + semaforoAdmision.availablePermits());
            RegistroMetricas.escribir("concurrencia_sismica.log",
                    RegistroMetricas.evento("SEMAFORO", "ESPERA_COLA", idEstacion,
                            "dispAntes=" + semaforoAdmision.availablePermits(),
                            "hilo=" + Thread.currentThread().getName()));

            if (!semaforoAdmision.tryAcquire()) {
                Thread.sleep(AVISO_BLOQUEO_MS);
                double esperaBloqueoMs = (System.nanoTime() - esperaInicialNs) / 1_000_000.0;
                System.out.println("[Bloqueo] Estacion " + idEstacion
                        + " no obtuvo permiso. Debe guardar lectura en cola local.");
                RegistroMetricas.escribir("concurrencia_sismica.log",
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
            RegistroMetricas.escribir("concurrencia_sismica.log",
                    RegistroMetricas.evento("SEMAFORO", "PERMISO_OTORGADO", idEstacion,
                            "esperaMs=" + RegistroMetricas.ms(esperaMs),
                            "dispDespues=" + semaforoAdmision.availablePermits(),
                            "hilo=" + Thread.currentThread().getName()));

            guardarEnBitacora(idEstacion, tramaDatos, timestampProcesamiento);
            Thread.sleep(retardoSimuladoMs);
            return ResultadoAdmision.admitido(esperaMs, semaforoAdmision.availablePermits());
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            RegistroMetricas.escribir("concurrencia_sismica.log",
                    RegistroMetricas.evento("ERROR", "INTERRUPCION_SEMAFORO", idEstacion,
                            "detalle=" + ex.getMessage()));
            return ResultadoAdmision.error(ex.getMessage(), semaforoAdmision.availablePermits());
        } finally {
            if (permisoAdquirido) {
                semaforoAdmision.release();
                System.out.println("[Liberacion] " + idEstacion + " libero el permiso del semaforo.");
                RegistroMetricas.escribir("concurrencia_sismica.log",
                        RegistroMetricas.evento("SEMAFORO", "PERMISO_LIBERADO", idEstacion,
                                "dispDespues=" + semaforoAdmision.availablePermits(),
                                "hilo=" + Thread.currentThread().getName()));
            }
        }
    }

    private synchronized void guardarEnBitacora(String idEstacion, String tramaDatos, long timestampProcesamiento) {
        RegistroMetricas.escribir("concurrencia_sismica.log",
                RegistroMetricas.evento("CRITICA", "ENTRA_BITACORA", idEstacion,
                        "monitor=synchronized", "archivo=" + archivoBitacora.getName(),
                        "hilo=" + Thread.currentThread().getName()));

        try (FileWriter fw = new FileWriter(archivoBitacora, true);
                BufferedWriter bw = new BufferedWriter(fw);
                PrintWriter out = new PrintWriter(bw)) {
            out.println("TS_SERV:" + timestampProcesamiento + ";" + tramaDatos);
            RegistroMetricas.escribir("concurrencia_sismica.log",
                    RegistroMetricas.evento("CRITICA", "ESCRITURA_OK", idEstacion,
                            "archivo=" + archivoBitacora.getName()));
        } catch (IOException ex) {
            RegistroMetricas.escribir("concurrencia_sismica.log",
                    RegistroMetricas.evento("ERROR", "ERROR_IO_BITACORA", idEstacion,
                            "error=" + ex.getClass().getSimpleName(), "detalle=" + ex.getMessage()));
        } finally {
            RegistroMetricas.escribir("concurrencia_sismica.log",
                    RegistroMetricas.evento("CRITICA", "SALE_BITACORA", idEstacion,
                            "monitor=synchronized", "hilo=" + Thread.currentThread().getName()));
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
