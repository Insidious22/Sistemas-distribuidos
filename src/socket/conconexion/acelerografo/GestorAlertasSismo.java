package socket.conconexion.acelerografo;

import java.io.PrintWriter;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import telemetria.RegistroMetricas;

public final class GestorAlertasSismo {
    private static final ConcurrentHashMap<String, SuscriptorAlerta> SUSCRIPTORES =
            new ConcurrentHashMap<String, SuscriptorAlerta>();
    private static final Semaphore CANAL_ALERTA = new Semaphore(1, true);
    private static final Semaphore CANAL_CONSULTA = new Semaphore(1, true);
    private static volatile SismoActivo sismoActivo;
    private static volatile String clienteEnConsulta = "-";

    private GestorAlertasSismo() {
    }

    public static void registrar(String idAcel, PrintWriter salida, String remoto) {
        SUSCRIPTORES.put(idAcel, new SuscriptorAlerta(idAcel, salida, remoto));
        RegistroMetricas.escribir("interbloqueo_sismo.log",
                RegistroMetricas.linea("CLIENTE_ALERTA_REGISTRADO", "idAcel=" + idAcel,
                        "remoto=" + remoto, "suscriptores=" + SUSCRIPTORES.size()));
    }

    public static void desregistrar(String idAcel) {
        SuscriptorAlerta removido = SUSCRIPTORES.remove(idAcel);
        if (removido != null) {
            RegistroMetricas.escribir("interbloqueo_sismo.log",
                    RegistroMetricas.linea("CLIENTE_ALERTA_DESCONECTADO", "idAcel=" + idAcel,
                            "remoto=" + removido.remoto, "suscriptores=" + SUSCRIPTORES.size()));
        }
    }

    public static void simularSismo(String idEvento, long duracionMs, long retardoPorClienteMs) {
        activarSismo(idEvento, duracionMs, retardoPorClienteMs);
        List<SuscriptorAlerta> suscriptores = new ArrayList<SuscriptorAlerta>(SUSCRIPTORES.values());
        RegistroMetricas.escribir("interbloqueo_sismo.log",
                RegistroMetricas.linea("SISMO_INICIADO", "idEvento=" + idEvento,
                        "duracionMs=" + duracionMs, "retardoPorClienteMs=" + retardoPorClienteMs,
                        "clientesObjetivo=" + suscriptores.size(), "inicio=" + Instant.now()));

        for (SuscriptorAlerta suscriptor : suscriptores) {
            Thread hilo = new Thread(new NotificadorAlerta(suscriptor, idEvento, duracionMs, retardoPorClienteMs),
                    "alerta-" + idEvento + "-" + suscriptor.idAcel);
            hilo.start();
        }
    }

    public static void consultarMovimiento(String idAcel, int prioridad, String remoto, PrintWriter salida,
            long retardoAtencionMs) throws InterruptedException {
        long inicioEsperaNs = System.nanoTime();
        boolean adquirido = false;

        // Pequenio desfase para que, en una prueba lanzada casi al mismo tiempo,
        // la estacion con prioridad 1 alcance primero el canal critico.
        if (prioridad > 1) {
            Thread.sleep(Math.min(300L, (prioridad - 1L) * 100L));
        }

        RegistroMetricas.escribir("interbloqueo_sismo.log",
                RegistroMetricas.linea("CLIENTE_DETECTA_MOVIMIENTO_Y_CONSULTA",
                        "idAcel=" + idAcel,
                        "prioridad=" + prioridad,
                        "remoto=" + remoto,
                        "clienteEnAtencion=" + clienteEnConsulta,
                        "permisosConsulta=" + CANAL_CONSULTA.availablePermits()));

        if (!CANAL_CONSULTA.tryAcquire()) {
            Thread.sleep(1000L);
            double esperaAvisoMs = (System.nanoTime() - inicioEsperaNs) / 1_000_000.0;
            String ocupante = clienteEnConsulta;
            salida.println("BLOQUEADO_CONSULTA;ID_ACEL:" + idAcel
                    + ";MOTIVO:CANAL_OCUPADO_POR_PRIORIDAD;CLIENTE_EN_ATENCION:" + ocupante
                    + ";ESPERA_AVISO_MS:" + RegistroMetricas.ms(esperaAvisoMs)
                    + ";SERVIDOR_AVISO:" + Instant.now());
            RegistroMetricas.escribir("interbloqueo_sismo.log",
                    RegistroMetricas.linea("CLIENTE_CONSULTA_BLOQUEADO",
                            "idAcel=" + idAcel,
                            "prioridad=" + prioridad,
                            "bloqueadoPor=" + ocupante,
                            "esperaAvisoMs=" + RegistroMetricas.ms(esperaAvisoMs),
                            "motivo=otro_cliente_recibe_informacion_sismica"));

            CANAL_CONSULTA.acquire();
            adquirido = true;
        } else {
            adquirido = true;
        }

        try {
            clienteEnConsulta = idAcel;
            double esperaTotalMs = (System.nanoTime() - inicioEsperaNs) / 1_000_000.0;
            MuestraMovimiento muestra = muestraActual();
            boolean hayMovimiento = muestra.activo;
            RegistroMetricas.escribir("interbloqueo_sismo.log",
                    RegistroMetricas.linea("CLIENTE_ENTRA_CONSULTA_SISMO",
                            "idAcel=" + idAcel,
                            "prioridad=" + prioridad,
                            "esperaTotalMs=" + RegistroMetricas.ms(esperaTotalMs),
                            "idEvento=" + muestra.idEvento,
                            "movimiento=" + (hayMovimiento ? "SI" : "NO"),
                            "xG=" + formato(muestra.xG),
                            "yG=" + formato(muestra.yG),
                            "zG=" + formato(muestra.zG),
                            "intensidad=" + formato(muestra.intensidad),
                            "retardoAtencionMs=" + retardoAtencionMs));

            salida.println("RESPUESTA_CONSULTA_SISMO;ID_ACEL:" + idAcel
                    + ";MOVIMIENTO:" + (hayMovimiento ? "SI" : "NO")
                    + ";ID_EVENTO:" + muestra.idEvento
                    + ";X_G:" + formato(muestra.xG)
                    + ";Y_G:" + formato(muestra.yG)
                    + ";Z_G:" + formato(muestra.zG)
                    + ";INTENSIDAD:" + formato(muestra.intensidad)
                    + ";DURACION_RESTANTE_MS:" + muestra.restanteMs
                    + ";ESPERA_TOTAL_MS:" + RegistroMetricas.ms(esperaTotalMs)
                    + ";SERVIDOR_RESPUESTA:" + Instant.now());

            RegistroMetricas.escribir("interbloqueo_sismo.log",
                    RegistroMetricas.linea("RESPUESTA_CONSULTA_SISMO_ENVIADA",
                            "idAcel=" + idAcel,
                            "prioridad=" + prioridad,
                            "movimiento=" + (hayMovimiento ? "SI" : "NO"),
                            "idEvento=" + muestra.idEvento,
                            "esperaTotalMs=" + RegistroMetricas.ms(esperaTotalMs)));

            Thread.sleep(retardoAtencionMs);
        } finally {
            if (adquirido) {
                clienteEnConsulta = "-";
                CANAL_CONSULTA.release();
                RegistroMetricas.escribir("interbloqueo_sismo.log",
                        RegistroMetricas.linea("CLIENTE_LIBERA_CONSULTA_SISMO",
                                "idAcel=" + idAcel,
                                "prioridad=" + prioridad,
                                "permisosConsulta=" + CANAL_CONSULTA.availablePermits()));
            }
        }
    }

    private static synchronized void activarSismo(String idEvento, long duracionMs, long retardoPorClienteMs) {
        long inicioMs = System.currentTimeMillis();
        SismoActivo nuevo = new SismoActivo(idEvento, inicioMs, inicioMs + duracionMs, retardoPorClienteMs);
        sismoActivo = nuevo;
        Thread hilo = new Thread(new MovimientoSismo(nuevo), "movimiento-" + idEvento);
        hilo.setDaemon(true);
        hilo.start();
    }

    private static MuestraMovimiento muestraActual() {
        SismoActivo actual = sismoActivo;
        long ahoraMs = System.currentTimeMillis();
        if (actual == null || ahoraMs > actual.finMs) {
            return new MuestraMovimiento(false, "-", 0L, 0.0, 0.0, 1.0, 0.0);
        }
        return calcularMuestra(actual, ahoraMs);
    }

    private static MuestraMovimiento calcularMuestra(SismoActivo actual, long ahoraMs) {
        long transcurridoMs = Math.max(0L, ahoraMs - actual.inicioMs);
        long segundo = transcurridoMs / 1000L;
        double xG = 0.020 + ((segundo % 5) * 0.017);
        double yG = -0.015 - ((segundo % 4) * 0.013);
        double zG = 0.980 + ((segundo % 6) * 0.021);
        double intensidad = Math.sqrt((xG * xG) + (yG * yG) + ((zG - 0.980) * (zG - 0.980)));
        long restanteMs = Math.max(0L, actual.finMs - ahoraMs);
        return new MuestraMovimiento(true, actual.idEvento, restanteMs, xG, yG, zG, intensidad);
    }

    private static String formato(double valor) {
        return String.format(Locale.US, "%.3f", valor);
    }

    private static final class NotificadorAlerta implements Runnable {
        private final SuscriptorAlerta suscriptor;
        private final String idEvento;
        private final long duracionMs;
        private final long retardoPorClienteMs;

        private NotificadorAlerta(SuscriptorAlerta suscriptor, String idEvento, long duracionMs,
                long retardoPorClienteMs) {
            this.suscriptor = suscriptor;
            this.idEvento = idEvento;
            this.duracionMs = duracionMs;
            this.retardoPorClienteMs = retardoPorClienteMs;
        }

        public void run() {
            long esperaInicioNs = System.nanoTime();
            boolean adquirido = false;
            try {
                RegistroMetricas.escribir("interbloqueo_sismo.log",
                        RegistroMetricas.linea("CLIENTE_BLOQUEADO_ESPERANDO_ALERTA",
                                "idEvento=" + idEvento,
                                "idAcel=" + suscriptor.idAcel,
                                "motivo=canal_alerta_ocupado_o_en_espera",
                                "permisosDisponibles=" + CANAL_ALERTA.availablePermits(),
                                "hilo=" + Thread.currentThread().getName()));

                CANAL_ALERTA.acquire();
                adquirido = true;
                double esperaMs = (System.nanoTime() - esperaInicioNs) / 1_000_000.0;
                Instant envio = Instant.now();

                RegistroMetricas.escribir("interbloqueo_sismo.log",
                        RegistroMetricas.linea("CLIENTE_ENTRA_CANAL_ALERTA",
                                "idEvento=" + idEvento,
                                "idAcel=" + suscriptor.idAcel,
                                "esperaMs=" + RegistroMetricas.ms(esperaMs),
                                "retardoPorClienteMs=" + retardoPorClienteMs,
                                "hilo=" + Thread.currentThread().getName()));

                suscriptor.enviar("ALERTA_SISMO;ID_EVENTO:" + idEvento + ";ID_ACEL:" + suscriptor.idAcel
                        + ";DURACION_MS:" + duracionMs + ";RETARDO_CLIENTE_MS:" + retardoPorClienteMs
                        + ";SERVIDOR_ENVIO:" + envio);

                RegistroMetricas.escribir("interbloqueo_sismo.log",
                        RegistroMetricas.linea("ALERTA_ENVIADA_A_CLIENTE",
                                "idEvento=" + idEvento,
                                "idAcel=" + suscriptor.idAcel,
                                "esperaMs=" + RegistroMetricas.ms(esperaMs),
                                "duracionMs=" + duracionMs,
                                "envio=" + envio));

                Thread.sleep(retardoPorClienteMs);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                RegistroMetricas.escribir("interbloqueo_sismo.log",
                        RegistroMetricas.linea("ALERTA_INTERRUMPIDA", "idEvento=" + idEvento,
                                "idAcel=" + suscriptor.idAcel, "detalle=" + ex.getMessage()));
            } finally {
                if (adquirido) {
                    CANAL_ALERTA.release();
                    RegistroMetricas.escribir("interbloqueo_sismo.log",
                            RegistroMetricas.linea("CLIENTE_LIBERA_CANAL_ALERTA",
                                    "idEvento=" + idEvento,
                                    "idAcel=" + suscriptor.idAcel,
                                    "permisosDisponibles=" + CANAL_ALERTA.availablePermits(),
                                    "hilo=" + Thread.currentThread().getName()));
                }
            }
        }
    }

    private static final class SuscriptorAlerta {
        private final String idAcel;
        private final PrintWriter salida;
        private final String remoto;

        private SuscriptorAlerta(String idAcel, PrintWriter salida, String remoto) {
            this.idAcel = idAcel;
            this.salida = salida;
            this.remoto = remoto;
        }

        private synchronized void enviar(String mensaje) {
            salida.println(mensaje);
        }
    }

    private static final class MovimientoSismo implements Runnable {
        private final SismoActivo evento;

        private MovimientoSismo(SismoActivo evento) {
            this.evento = evento;
        }

        public void run() {
            int muestra = 1;
            while (System.currentTimeMillis() <= evento.finMs && sismoActivo == evento) {
                MuestraMovimiento movimiento = calcularMuestra(evento, System.currentTimeMillis());
                RegistroMetricas.escribir("interbloqueo_sismo.log",
                        RegistroMetricas.linea("SISMO_MOVIMIENTO_VARIA",
                                "idEvento=" + evento.idEvento,
                                "muestra=" + muestra,
                                "xG=" + formato(movimiento.xG),
                                "yG=" + formato(movimiento.yG),
                                "zG=" + formato(movimiento.zG),
                                "intensidad=" + formato(movimiento.intensidad),
                                "restanteMs=" + movimiento.restanteMs));
                muestra++;
                try {
                    Thread.sleep(1000L);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            if (sismoActivo == evento) {
                sismoActivo = null;
                RegistroMetricas.escribir("interbloqueo_sismo.log",
                        RegistroMetricas.linea("SISMO_FINALIZADO", "idEvento=" + evento.idEvento,
                                "duracionMs=" + (evento.finMs - evento.inicioMs)));
            }
        }
    }

    private static final class SismoActivo {
        private final String idEvento;
        private final long inicioMs;
        private final long finMs;
        @SuppressWarnings("unused")
        private final long retardoPorClienteMs;

        private SismoActivo(String idEvento, long inicioMs, long finMs, long retardoPorClienteMs) {
            this.idEvento = idEvento;
            this.inicioMs = inicioMs;
            this.finMs = finMs;
            this.retardoPorClienteMs = retardoPorClienteMs;
        }
    }

    private static final class MuestraMovimiento {
        private final boolean activo;
        private final String idEvento;
        private final long restanteMs;
        private final double xG;
        private final double yG;
        private final double zG;
        private final double intensidad;

        private MuestraMovimiento(boolean activo, String idEvento, long restanteMs, double xG, double yG,
                double zG, double intensidad) {
            this.activo = activo;
            this.idEvento = idEvento;
            this.restanteMs = restanteMs;
            this.xG = xG;
            this.yG = yG;
            this.zG = zG;
            this.intensidad = intensidad;
        }
    }
}
