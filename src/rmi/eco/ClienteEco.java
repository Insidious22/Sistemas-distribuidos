package rmi.eco;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Arrays;

public class ClienteEco {
    private static final int PUERTO_DEFECTO = 1099;
    private static final String HOST_DEFECTO = "localhost";
    private static final String SERVICIO_DEFECTO = "eco";

    public static void main(String[] args) {
        try {
            String host = args.length >= 1 ? args[0] : leerTexto("Nodo del registro RMI", HOST_DEFECTO);
            int puerto = args.length >= 2
                    ? Integer.parseInt(args[1])
                    : leerEntero("Puerto del registro RMI", PUERTO_DEFECTO);
            String nombreServicio = args.length >= 3 ? args[2] : SERVICIO_DEFECTO;
            String[] mensajes = args.length >= 4
                    ? Arrays.copyOfRange(args, 3, args.length)
                    : new String[] {"Hola!", "Adios!"};

            Registry registro = LocateRegistry.getRegistry(host, puerto);
            ServicioEco servicio = (ServicioEco) registro.lookup(nombreServicio);

            System.out.println("Busqueda completa");
            System.out.println("Servicio de Eco:");
            for (String mensaje : mensajes) {
                System.out.println(servicio.eco(mensaje));
            }
        } catch (RemoteException ex) {
            System.err.println("Error de comunicacion: " + ex);
        } catch (Exception ex) {
            System.err.println("Excepcion en ClienteEco:");
            ex.printStackTrace();
        }
    }

    private static String leerTexto(String mensaje, String valorDefecto) throws Exception {
        System.out.print(mensaje + " [" + valorDefecto + "]: ");
        String valor = new BufferedReader(new InputStreamReader(System.in)).readLine();
        if (valor == null || valor.trim().isEmpty()) {
            return valorDefecto;
        }
        return valor.trim();
    }

    private static int leerEntero(String mensaje, int valorDefecto) throws Exception {
        return Integer.parseInt(leerTexto(mensaje, Integer.toString(valorDefecto)));
    }
}
