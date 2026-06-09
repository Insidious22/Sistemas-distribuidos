package telemetria;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.Locale;

public final class RegistroMetricas {
    private static final Path CARPETA_LOGS = Paths.get("logs");

    private RegistroMetricas() {
    }

    public static synchronized void escribir(String archivo, String linea) {
        try {
            Files.createDirectories(CARPETA_LOGS);
            Path ruta = CARPETA_LOGS.resolve(archivo);
            Files.write(ruta, (linea + System.lineSeparator()).getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException ex) {
            System.err.println("No se pudo escribir log " + archivo + ": " + ex.getMessage());
        }
    }

    public static String linea(String evento, String... campos) {
        StringBuilder builder = new StringBuilder();
        builder.append(ahora()).append(" | ").append(evento);
        for (String campo : campos) {
            if (campo == null || campo.trim().isEmpty()) {
                continue;
            }
            builder.append(" | ").append(campo);
        }
        return builder.toString();
    }

    public static String evento(String categoria, String accion, String id, String... campos) {
        StringBuilder builder = new StringBuilder();
        builder.append(ahora()).append(" | ");
        builder.append(ajustar("[" + categoria + "]", 14)).append(" | ");
        builder.append(ajustar(accion, 28)).append(" | ");
        builder.append("id=").append(ajustar(id, 14));
        for (String campo : campos) {
            if (campo == null || campo.trim().isEmpty()) {
                continue;
            }
            builder.append(" | ").append(campo);
        }
        return builder.toString();
    }

    public static String ahora() {
        return Instant.now().toString();
    }

    public static String ms(double valor) {
        return String.format(Locale.US, "%.4f", valor);
    }

    private static String ajustar(String valor, int longitud) {
        String texto = valor == null ? "" : valor;
        if (texto.length() >= longitud) {
            return texto;
        }
        StringBuilder builder = new StringBuilder(texto);
        while (builder.length() < longitud) {
            builder.append(' ');
        }
        return builder.toString();
    }

}
