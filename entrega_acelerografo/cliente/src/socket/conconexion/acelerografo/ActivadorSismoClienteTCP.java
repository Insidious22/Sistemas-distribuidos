package socket.conconexion.acelerografo;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.time.Instant;
import telemetria.RegistroMetricas;

public class ActivadorSismoClienteTCP {
    private static final String LOG_SISMO = "sismo_distribuido.log";
    private static final String ARCHIVO_SISMO = "sismo_manual.txt";

    public static void main(String[] args) {
        String idEvento = args.length >= 1 ? args[0] : "SISMO-MANUAL-" + System.currentTimeMillis();
        long duracionMs = args.length >= 2 ? Long.parseLong(args[1]) : 30000L;
        long inicioMs = System.currentTimeMillis();

        File archivo = new File(ARCHIVO_SISMO);
        try (BufferedWriter out = new BufferedWriter(new FileWriter(archivo, false))) {
            out.write("ID_EVENTO=" + idEvento);
            out.newLine();
            out.write("INICIO_MS=" + inicioMs);
            out.newLine();
            out.write("DURACION_MS=" + duracionMs);
            out.newLine();
            out.write("ACTIVADO_EN=" + Instant.ofEpochMilli(inicioMs));
            out.newLine();

            System.out.println("Sismo local activado para clientes de esta maquina.");
            System.out.println("Evento: " + idEvento);
            System.out.println("Duracion: " + duracionMs + " ms");
            System.out.println("Archivo: " + archivo.getAbsolutePath());

            RegistroMetricas.escribir(LOG_SISMO,
                    RegistroMetricas.linea("SISMO_LOCAL_CLIENTE_ACTIVADO",
                            "idEvento=" + idEvento,
                            "duracionMs=" + duracionMs,
                            "archivo=" + archivo.getAbsolutePath()));
        } catch (Exception ex) {
            RegistroMetricas.escribir(LOG_SISMO,
                    RegistroMetricas.linea("SISMO_LOCAL_CLIENTE_ERROR",
                            "idEvento=" + idEvento,
                            "error=" + ex.getClass().getSimpleName(),
                            "detalle=" + ex.getMessage()));
            ex.printStackTrace();
        }
    }
}
