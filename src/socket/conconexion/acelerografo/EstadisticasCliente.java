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
    private double sumaRedEstimadaMs;
    private double sumaIdaEstimadaMs;
    private double sumaVueltaEstimadaMs;
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

    public synchronized void registrarMetricaCliente(double rttMs, double redEstimadaMs, double idaEstimadaMs,
            double vueltaEstimadaMs) {
        registrarRttCliente(rttMs);
        sumaRedEstimadaMs += redEstimadaMs;
        sumaIdaEstimadaMs += idaEstimadaMs;
        sumaVueltaEstimadaMs += vueltaEstimadaMs;
    }

    public synchronized String resumen() {
        double promedioRtt = metricasReportadas == 0 ? 0.0 : sumaRttMs / metricasReportadas;
        double promedioProcesamiento = lecturasRecibidas == 0 ? 0.0 : sumaProcesamientoMs / lecturasRecibidas;
        double promedioRed = metricasReportadas == 0 ? 0.0 : sumaRedEstimadaMs / metricasReportadas;
        double promedioIda = metricasReportadas == 0 ? 0.0 : sumaIdaEstimadaMs / metricasReportadas;
        double promedioVuelta = metricasReportadas == 0 ? 0.0 : sumaVueltaEstimadaMs / metricasReportadas;
        double minimo = metricasReportadas == 0 ? 0.0 : minimoRttMs;
        double maximo = metricasReportadas == 0 ? 0.0 : maximoRttMs;
        return String.format(Locale.US,
                "idAcel=%s | cliente=%s | lecturas=%d | metricasRtt=%d | rttPromedioMs=%.4f | rttMinMs=%.4f | rttMaxMs=%.4f | redEstimadaPromedioMs=%.4f | idaEstimadaPromedioMs=%.4f | vueltaEstimadaPromedioMs=%.4f | procesamientoPromedioMs=%.4f | ultimaLecturaNs=%d",
                idAcel, direccionRemota, lecturasRecibidas, metricasReportadas, promedioRtt, minimo, maximo,
                promedioRed, promedioIda, promedioVuelta, promedioProcesamiento, ultimaLecturaNs);
    }

    public synchronized String[] resumenCampos() {
        double promedioRtt = metricasReportadas == 0 ? 0.0 : sumaRttMs / metricasReportadas;
        double promedioProcesamiento = lecturasRecibidas == 0 ? 0.0 : sumaProcesamientoMs / lecturasRecibidas;
        double promedioRed = metricasReportadas == 0 ? 0.0 : sumaRedEstimadaMs / metricasReportadas;
        double promedioIda = metricasReportadas == 0 ? 0.0 : sumaIdaEstimadaMs / metricasReportadas;
        double promedioVuelta = metricasReportadas == 0 ? 0.0 : sumaVueltaEstimadaMs / metricasReportadas;
        double minimo = metricasReportadas == 0 ? 0.0 : minimoRttMs;
        double maximo = metricasReportadas == 0 ? 0.0 : maximoRttMs;
        return new String[] {
                "lecturas=" + lecturasRecibidas,
                "metricas=" + metricasReportadas,
                "rttPromMs=" + String.format(Locale.US, "%.4f", promedioRtt),
                "rttMinMs=" + String.format(Locale.US, "%.4f", minimo),
                "rttMaxMs=" + String.format(Locale.US, "%.4f", maximo),
                "redPromMs=" + String.format(Locale.US, "%.4f", promedioRed),
                "idaPromMs=" + String.format(Locale.US, "%.4f", promedioIda),
                "vueltaPromMs=" + String.format(Locale.US, "%.4f", promedioVuelta),
                "procPromMs=" + String.format(Locale.US, "%.4f", promedioProcesamiento)
        };
    }
}
