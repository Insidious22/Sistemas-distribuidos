package socket.conconexion.acelerografo;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public class LecturaTriaxial {
    private final String idAcel;
    private final double ejeX;
    private final double ejeY;
    private final double ejeZ;
    private final String timestamp;

    public LecturaTriaxial(String idAcel, double ejeX, double ejeY, double ejeZ, String timestamp) {
        this.idAcel = idAcel;
        this.ejeX = ejeX;
        this.ejeY = ejeY;
        this.ejeZ = ejeZ;
        this.timestamp = timestamp;
    }

    public static LecturaTriaxial desdePayload(String payload) {
        Map<String, String> campos = new HashMap<String, String>();
        for (String parte : payload.split(";")) {
            int separador = parte.indexOf(':');
            if (separador > 0) {
                campos.put(parte.substring(0, separador), parte.substring(separador + 1));
            }
        }

        String idAcel = requerir(campos, "ID_ACEL");
        double ejeX = leerGravedad(requerir(campos, "EJE_X"));
        double ejeY = leerGravedad(requerir(campos, "EJE_Y"));
        double ejeZ = leerGravedad(requerir(campos, "EJE_Z"));
        String timestamp = requerir(campos, "TIMESTAMP");
        validarTimestamp(timestamp);

        return new LecturaTriaxial(idAcel, ejeX, ejeY, ejeZ, timestamp);
    }

    public static String crearPayload(String idAcel, double ejeX, double ejeY, double ejeZ) {
        return String.format(Locale.US,
                "ID_ACEL:%s;EJE_X:%+.3fG;EJE_Y:%+.3fG;EJE_Z:%+.3fG;TIMESTAMP:%s",
                idAcel, ejeX, ejeY, ejeZ, Instant.now().toString());
    }

    public String getIdAcel() {
        return idAcel;
    }

    @Override
    public String toString() {
        return String.format(Locale.US, "%s X=%+.3fG Y=%+.3fG Z=%+.3fG TIMESTAMP=%s",
                idAcel, ejeX, ejeY, ejeZ, timestamp);
    }

    private static String requerir(Map<String, String> campos, String nombre) {
        String valor = campos.get(nombre);
        if (valor == null || valor.trim().isEmpty()) {
            throw new IllegalArgumentException("Falta campo obligatorio: " + nombre);
        }
        return valor.trim();
    }

    private static double leerGravedad(String valor) {
        String normalizado = valor.trim().toUpperCase(Locale.ROOT);
        if (!normalizado.endsWith("G")) {
            throw new IllegalArgumentException("La lectura debe terminar en G: " + valor);
        }
        return Double.parseDouble(normalizado.substring(0, normalizado.length() - 1));
    }

    private static void validarTimestamp(String timestamp) {
        try {
            Instant.parse(timestamp);
        } catch (DateTimeParseException ex) {
            throw new IllegalArgumentException("TIMESTAMP invalido: " + timestamp, ex);
        }
    }
}
