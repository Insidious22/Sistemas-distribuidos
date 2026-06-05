package rmi.sum;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class ClienteSum {
    private static final int PUERTO_DEFECTO = 1099;
    private static final String HOST_DEFECTO = "localhost";
    private static final String SERVICIO_DEFECTO = "sum";
    private static final int PRIMER_NUMERO_DEFECTO = 5;
    private static final int SEGUNDO_NUMERO_DEFECTO = 7;

    public static void main(String[] args) {
        try {
            String host = args.length >= 1 ? args[0] : leerTexto("Nodo del registro RMI", HOST_DEFECTO);
            int puerto = args.length >= 2
                    ? Integer.parseInt(args[1])
                    : leerEntero("Puerto del registro RMI", PUERTO_DEFECTO);
            String nombreServicio = args.length >= 3 ? args[2] : SERVICIO_DEFECTO;
            int primerNumero = args.length >= 4
                    ? Integer.parseInt(args[3])
                    : leerEntero("Primer numero", PRIMER_NUMERO_DEFECTO);
            int segundoNumero = args.length >= 5
                    ? Integer.parseInt(args[4])
                    : leerEntero("Segundo numero", SEGUNDO_NUMERO_DEFECTO);

            Registry registro = LocateRegistry.getRegistry(host, puerto);
            ServicioSum servicio = (ServicioSum) registro.lookup(nombreServicio);
            int resultado = servicio.sumar(primerNumero, segundoNumero);

            System.out.println("Busqueda completa");
            System.out.println("Servicio de Sum:");
            System.out.println(primerNumero + " + " + segundoNumero + " = " + resultado);
        } catch (RemoteException ex) {
            System.err.println("Error de comunicacion: " + ex);
        } catch (Exception ex) {
            System.err.println("Excepcion en ClienteSum:");
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
