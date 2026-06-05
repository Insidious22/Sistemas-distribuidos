package socket.conconexion.acelerografo;

import java.util.Locale;

public class EstadisticasCliente {
    private final String idAcel;
    private final String direccionRemota;
    private int lecturasRecibidas;
    private int metricasReportadas;
    private double sumaRttMs;
    private double minimoRttMs = Double.MAX_VALUE;
    private double maximoRttMs = Double.MIN_VALUE;
    private double sumaProcesamientoMs;
    private long ultimaLecturaNs;

    public EstadisticasCliente(String idAcel, String direccionRemota) {
        this.idAcel = idAcel;
        this.direccionRemota = direccionRemota;
    }

    public synchronized void registrarLectura(double procesamientoMs) {
        lecturasRecibidas++;
        sumaProcesamientoMs += procesamientoMs;
        ultimaLecturaNs = System.nanoTime();
    }

    public synchronized void registrarRttCliente(double rttMs) {
        metricasReportadas++;
        sumaRttMs += rttMs;
        minimoRttMs = Math.min(minimoRttMs, rttMs);
        maximoRttMs = Math.max(maximoRttMs, rttMs);
    }

    public synchronized String resumen() {
        double promedioRtt = metricasReportadas == 0 ? 0.0 : sumaRttMs / metricasReportadas;
        double promedioProcesamiento = lecturasRecibidas == 0 ? 0.0 : sumaProcesamientoMs / lecturasRecibidas;
        double minimo = metricasReportadas == 0 ? 0.0 : minimoRttMs;
        double maximo = metricasReportadas == 0 ? 0.0 : maximoRttMs;
        return String.format(Locale.US,
                "idAcel=%s | cliente=%s | lecturas=%d | metricasRtt=%d | rttPromedioMs=%.4f | rttMinMs=%.4f | rttMaxMs=%.4f | procesamientoPromedioMs=%.4f | ultimaLecturaNs=%d",
                idAcel, direccionRemota, lecturasRecibidas, metricasReportadas, promedioRtt, minimo, maximo,
                promedioProcesamiento, ultimaLecturaNs);
    }
}
