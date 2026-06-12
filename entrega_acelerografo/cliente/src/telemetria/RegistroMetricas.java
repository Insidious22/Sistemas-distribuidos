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
    private static final int ANCHO_TABLA = 96;
    private static final int ANCHO_CAMPO = 24;

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
        agregarSeparador(builder);
        agregarFilaCompleta(builder, "EVENTO " + evento);
        agregarSeparador(builder);
        agregarFila(builder, "timestamp", ahora());
        agregarFila(builder, "evento", evento);
        for (String campo : campos) {
            agregarCampoCompuesto(builder, campo);
        }
        agregarSeparador(builder);
        return builder.toString();
    }

    public static String evento(String categoria, String accion, String id, String... campos) {
        String titulo = "[" + categoria + "] " + accion;
        StringBuilder builder = new StringBuilder();
        agregarSeparador(builder);
        agregarFilaCompleta(builder, titulo);
        agregarSeparador(builder);
        agregarFila(builder, "timestamp", ahora());
        agregarFila(builder, "categoria", categoria);
        agregarFila(builder, "accion", accion);
        agregarFila(builder, "id", id);
        for (String campo : campos) {
            agregarCampoCompuesto(builder, campo);
        }
        agregarSeparador(builder);
        return builder.toString();
    }

    public static String lineaSimple(String evento, String... campos) {
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

    public static String eventoSimple(String categoria, String accion, String id, String... campos) {
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

    private static void agregarSeparador(StringBuilder builder) {
        builder.append('+');
        for (int i = 0; i < ANCHO_TABLA - 2; i++) {
            builder.append('-');
        }
        builder.append('+').append(System.lineSeparator());
    }

    private static void agregarFilaCompleta(StringBuilder builder, String valor) {
        agregarLineaCortada(builder, "", valor);
    }

    private static void agregarFila(StringBuilder builder, String campo, String valor) {
        agregarLineaCortada(builder, campo, valor);
    }

    private static void agregarCampoCompuesto(StringBuilder builder, String campoCompuesto) {
        if (campoCompuesto == null || campoCompuesto.trim().isEmpty()) {
            return;
        }
        String[] partes = campoCompuesto.split("\\s\\|\\s");
        if (partes.length > 1) {
            for (String parte : partes) {
                agregarCampoCompuesto(builder, parte);
            }
            return;
        }

        int separador = campoCompuesto.indexOf('=');
        if (separador > 0) {
            agregarFila(builder, campoCompuesto.substring(0, separador), campoCompuesto.substring(separador + 1));
        } else {
            agregarFila(builder, "detalle", campoCompuesto);
        }
    }

    private static void agregarLineaCortada(StringBuilder builder, String campo, String valor) {
        String etiqueta = campo == null ? "" : campo.trim();
        String texto = valor == null ? "" : valor.trim();
        int anchoValor = ANCHO_TABLA - ANCHO_CAMPO - 7;
        if (texto.isEmpty()) {
            agregarLinea(builder, etiqueta, "");
            return;
        }
        int inicio = 0;
        boolean primera = true;
        while (inicio < texto.length()) {
            int fin = Math.min(inicio + anchoValor, texto.length());
            agregarLinea(builder, primera ? etiqueta : "", texto.substring(inicio, fin));
            inicio = fin;
            primera = false;
        }
    }

    private static void agregarLinea(StringBuilder builder, String campo, String valor) {
        String etiqueta = ajustar(campo, ANCHO_CAMPO);
        int anchoValor = ANCHO_TABLA - ANCHO_CAMPO - 7;
        String texto = ajustar(valor, anchoValor);
        builder.append("| ").append(etiqueta).append(" | ").append(texto).append(" |")
                .append(System.lineSeparator());
    }

}
