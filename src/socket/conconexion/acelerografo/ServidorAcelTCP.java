package socket.conconexion.acelerografo;

import java.net.ServerSocket;
import java.net.Socket;

public class ServidorAcelTCP {
    private static final int PUERTO_DEFECTO = 7500;
    private static final int PERMISOS_DEFECTO = 3;

    public static void main(String[] args) {
        int puerto = args.length >= 1 ? Integer.parseInt(args[0]) : PUERTO_DEFECTO;
        int permisosConcurrentes = args.length >= 2 ? Integer.parseInt(args[1]) : PERMISOS_DEFECTO;
        HiloDespachador.configurarAdministradorRecursos(new AdministradorRecursosSismicos(permisosConcurrentes));

        try (ServerSocket servidor = new ServerSocket(puerto)) {
            System.out.println("Servidor TCP de acelerografos escuchando en puerto " + puerto);
            System.out.println("Semaforo de admision configurado con " + permisosConcurrentes + " permiso(s).");
            System.out.println("Informe a los clientes la IP LAN de esta maquina. Use Ctrl + C para detener.");

            while (true) {
                Socket socketCliente = servidor.accept();
                Thread hilo = new Thread(new HiloDespachador(socketCliente), "hilo-acel-" + socketCliente.getPort());
                hilo.start();
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
