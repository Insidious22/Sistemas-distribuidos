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
    private static final long RETARDO_SIMULADO_MS = 500L;

    private final Semaphore semaforoAdmision;
    private final File archivoBitacora = new File("bitacora_sismica.txt");

    public AdministradorRecursosSismicos() {
        this(PERMISOS_CONCURRENTES);
    }

    public AdministradorRecursosSismicos(int permisosConcurrentes) {
        semaforoAdmision = new Semaphore(permisosConcurrentes, true);
        RegistroMetricas.escribir("concurrencia_sismica.log",
                RegistroMetricas.linea("SEMAFORO_CONFIGURADO", "permisosConcurrentes=" + permisosConcurrentes,
                        "fairness=true", "retardoSimuladoMs=" + RETARDO_SIMULADO_MS));
    }

    public void procesarLecturaEstacion(String idEstacion, String tramaDatos) {
        boolean permisoAdquirido = false;
        long esperaInicialNs = System.nanoTime();

        try {
            System.out.println("[Control] Estacion " + idEstacion + " esperando permiso del semaforo. Permisos disponibles: "
                    + semaforoAdmision.availablePermits());
            RegistroMetricas.escribir("concurrencia_sismica.log",
                    RegistroMetricas.linea("COLA_SEMAFORO_ESPERA", "idEstacion=" + idEstacion,
                            "permisosDisponibles=" + semaforoAdmision.availablePermits(),
                            "hilo=" + Thread.currentThread().getName()));

            semaforoAdmision.acquire();
            permisoAdquirido = true;

            long esperaFinalNs = System.nanoTime();
            double esperaMs = (esperaFinalNs - esperaInicialNs) / 1_000_000.0;
            long timestampProcesamiento = System.currentTimeMillis();

            System.out.println("[Admision] Permiso concedido a " + idEstacion + ". Espera: "
                    + RegistroMetricas.ms(esperaMs) + " ms");
            RegistroMetricas.escribir("concurrencia_sismica.log",
                    RegistroMetricas.linea("SEMAFORO_PERMISO_CONCEDIDO", "idEstacion=" + idEstacion,
                            "esperaMs=" + RegistroMetricas.ms(esperaMs),
                            "permisosDisponibles=" + semaforoAdmision.availablePermits(),
                            "hilo=" + Thread.currentThread().getName()));

            guardarEnBitacora(idEstacion, tramaDatos, timestampProcesamiento);
            Thread.sleep(RETARDO_SIMULADO_MS);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            RegistroMetricas.escribir("concurrencia_sismica.log",
                    RegistroMetricas.linea("SEMAFORO_INTERRUPCION", "idEstacion=" + idEstacion,
                            "detalle=" + ex.getMessage()));
        } finally {
            if (permisoAdquirido) {
                semaforoAdmision.release();
                System.out.println("[Liberacion] " + idEstacion + " libero el permiso del semaforo.");
                RegistroMetricas.escribir("concurrencia_sismica.log",
                        RegistroMetricas.linea("SEMAFORO_PERMISO_LIBERADO", "idEstacion=" + idEstacion,
                                "permisosDisponibles=" + semaforoAdmision.availablePermits(),
                                "hilo=" + Thread.currentThread().getName()));
            }
        }
    }

    private synchronized void guardarEnBitacora(String idEstacion, String tramaDatos, long timestampProcesamiento) {
        RegistroMetricas.escribir("concurrencia_sismica.log",
                RegistroMetricas.linea("REGION_CRITICA_ENTRADA", "idEstacion=" + idEstacion,
                        "monitor=synchronized", "archivo=" + archivoBitacora.getName(),
                        "hilo=" + Thread.currentThread().getName()));

        try (FileWriter fw = new FileWriter(archivoBitacora, true);
                BufferedWriter bw = new BufferedWriter(fw);
                PrintWriter out = new PrintWriter(bw)) {
            out.println("TS_SERV:" + timestampProcesamiento + ";" + tramaDatos);
            RegistroMetricas.escribir("concurrencia_sismica.log",
                    RegistroMetricas.linea("REGION_CRITICA_ESCRITURA_SEGURA", "idEstacion=" + idEstacion,
                            "archivo=" + archivoBitacora.getName()));
        } catch (IOException ex) {
            RegistroMetricas.escribir("concurrencia_sismica.log",
                    RegistroMetricas.linea("REGION_CRITICA_ERROR_IO", "idEstacion=" + idEstacion,
                            "error=" + ex.getClass().getSimpleName(), "detalle=" + ex.getMessage()));
        } finally {
            RegistroMetricas.escribir("concurrencia_sismica.log",
                    RegistroMetricas.linea("REGION_CRITICA_SALIDA", "idEstacion=" + idEstacion,
                            "monitor=synchronized", "hilo=" + Thread.currentThread().getName()));
        }
    }
}
