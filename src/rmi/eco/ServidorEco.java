package rmi.eco;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class ServidorEco {
    private static final int PUERTO_DEFECTO = 1099;
    private static final String SERVICIO_DEFECTO = "eco";

    public static void main(String[] args) {
        try {
            int puerto = args.length >= 1
                    ? Integer.parseInt(args[0])
                    : leerEntero("Puerto del registro RMI", PUERTO_DEFECTO);
            String nombreServicio = args.length >= 2 ? args[1] : SERVICIO_DEFECTO;

            Registry registro = arrancarRegistro(puerto);
            ServicioEco servicio = new ServicioEcoImpl();
            registro.rebind(nombreServicio, servicio);

            System.out.println("Servidor Eco preparado");
            System.out.println("URL: rmi://localhost:" + puerto + "/" + nombreServicio);
            listarRegistro(registro, puerto);
            mantenerServidorActivo();
        } catch (RemoteException ex) {
            System.err.println("Error de comunicacion: " + ex);
            System.exit(1);
        } catch (Exception ex) {
            System.err.println("Excepcion en ServidorEco:");
            ex.printStackTrace();
            System.exit(1);
        }
    }

    private static Registry arrancarRegistro(int puerto) throws RemoteException {
        try {
            Registry registro = LocateRegistry.getRegistry(puerto);
            registro.list();
            System.out.println("Registro RMI localizado en el puerto " + puerto);
            return registro;
        } catch (RemoteException ex) {
            System.out.println("El registro RMI no se localiza en el puerto " + puerto);
            Registry registro = LocateRegistry.createRegistry(puerto);
            System.out.println("Registro RMI creado en el puerto " + puerto);
            return registro;
        }
    }

    private static void listarRegistro(Registry registro, int puerto) throws RemoteException {
        System.out.println("Registro rmi://localhost:" + puerto + " contiene:");
        for (String nombre : registro.list()) {
            System.out.println(nombre);
        }
    }

    private static void mantenerServidorActivo() throws InterruptedException {
        System.out.println("Servidor activo. Use Ctrl + C para detenerlo.");
        Thread.currentThread().join();
    }

    private static int leerEntero(String mensaje, int valorDefecto) throws Exception {
        System.out.print(mensaje + " [" + valorDefecto + "]: ");
        String valor = new BufferedReader(new InputStreamReader(System.in)).readLine();
        if (valor == null || valor.trim().isEmpty()) {
            return valorDefecto;
        }
        return Integer.parseInt(valor.trim());
    }
}
