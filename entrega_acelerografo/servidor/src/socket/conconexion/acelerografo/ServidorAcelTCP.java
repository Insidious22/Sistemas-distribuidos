package socket.conconexion.acelerografo;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import telemetria.RegistroMetricas;

public class ServidorAcelTCP {
    private static final String LOG_SISMO = "sismo_distribuido.log";
    private static final int PUERTO_DEFECTO = 7500;
    private static final int PERMISOS_DEFECTO = 3;
    private static final String ID_SERVIDOR_DEFECTO = "SERVIDOR-ACEL";
    private static final int PRIORIDAD_DEFECTO = 1;
    private static final long RETARDO_CRITICO_DEFECTO_MS = 500L;

    public static void main(String[] args) {
        int puerto = args.length >= 1 ? Integer.parseInt(args[0]) : PUERTO_DEFECTO;
        int permisosConcurrentes = args.length >= 2 ? Integer.parseInt(args[1]) : PERMISOS_DEFECTO;
        String idServidor = args.length >= 3 ? args[2] : ID_SERVIDOR_DEFECTO + "-" + puerto;
        int prioridadServidor = args.length >= 4 ? Integer.parseInt(args[3]) : PRIORIDAD_DEFECTO;
        long retardoCriticoMs = args.length >= 5 ? Long.parseLong(args[4]) : RETARDO_CRITICO_DEFECTO_MS;
        String replicas = args.length >= 6 ? args[5] : "";
        HiloDespachador.configurarAdministradorRecursos(
                new AdministradorRecursosSismicos(permisosConcurrentes, retardoCriticoMs));
        HiloDespachador.configurarNodoServidor(idServidor, prioridadServidor, puerto);
        HiloDespachador.configurarReplicas(replicas);

        try (ServerSocket servidor = new ServerSocket(puerto)) {
            System.out.println("Servidor TCP de acelerografos escuchando en puerto " + puerto);
            System.out.println("Nodo de redundancia: " + idServidor + " prioridad=" + prioridadServidor);
            System.out.println("Semaforo de admision configurado con " + permisosConcurrentes + " permiso(s).");
            System.out.println("Retardo de zona critica configurado en " + retardoCriticoMs + " ms.");
            if (!replicas.trim().isEmpty()) {
                System.out.println("Replicacion automatica hacia: " + replicas);
            }
            System.out.println("Informe a los clientes la IP LAN de esta maquina. Use Ctrl + C para detener.");

            while (!servidor.isClosed()) {
                try {
                    Socket socketCliente = servidor.accept();
                    Thread hilo = new Thread(new HiloDespachador(socketCliente), "hilo-acel-" + socketCliente.getPort());
                    hilo.setUncaughtExceptionHandler((thread, error) -> {
                        RegistroMetricas.escribir(LOG_SISMO,
                                RegistroMetricas.linea("TCP_HILO_ERROR_NO_CONTROLADO", "hilo=" + thread.getName(),
                                        "error=" + error.getClass().getSimpleName(),
                                        "detalle=" + error.getMessage()));
                        System.out.println("Un hilo de cliente fallo, pero el servidor sigue activo: "
                                + thread.getName());
                    });
                    hilo.start();
                } catch (IOException ex) {
                    RegistroMetricas.escribir(LOG_SISMO,
                            RegistroMetricas.linea("TCP_ACCEPT_ERROR", "error=" + ex.getClass().getSimpleName(),
                                    "detalle=" + ex.getMessage()));
                    System.out.println("No se pudo aceptar una conexion. El servidor seguira escuchando.");
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
