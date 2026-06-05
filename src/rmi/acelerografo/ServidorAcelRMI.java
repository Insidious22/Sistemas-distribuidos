package rmi.acelerografo;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Enumeration;

public class ServidorAcelRMI {
    private static final int PUERTO_DEFECTO = 1099;
    private static final String SERVICIO_DEFECTO = "acelerografo";

    public static void main(String[] args) {
        try {
            int puerto = args.length >= 1 ? Integer.parseInt(args[0]) : PUERTO_DEFECTO;
            String nombreServicio = args.length >= 2 ? args[1] : SERVICIO_DEFECTO;
            String ipPublicada = args.length >= 3 ? args[2] : detectarIpLan();

            System.setProperty("java.rmi.server.hostname", ipPublicada);

            Registry registro = arrancarRegistro(puerto);
            ServicioAcelerografo servicio = new ServicioAcelerografoImpl();
            registro.rebind(nombreServicio, servicio);

            System.out.println("Servidor RMI de acelerografos preparado");
            System.out.println("IP publicada para clientes: " + ipPublicada);
            System.out.println("URL: rmi://" + ipPublicada + ":" + puerto + "/" + nombreServicio);
            listarRegistro(registro, ipPublicada, puerto);
            mantenerServidorActivo();
        } catch (RemoteException ex) {
            System.err.println("Error de comunicacion: " + ex);
            System.exit(1);
        } catch (Exception ex) {
            System.err.println("Excepcion en ServidorAcelRMI:");
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

    private static void listarRegistro(Registry registro, String ipPublicada, int puerto) throws RemoteException {
        System.out.println("Registro rmi://" + ipPublicada + ":" + puerto + " contiene:");
        for (String nombre : registro.list()) {
            System.out.println(nombre);
        }
    }

    private static String detectarIpLan() throws Exception {
        Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
        while (interfaces.hasMoreElements()) {
            NetworkInterface interfaz = interfaces.nextElement();
            if (!interfaz.isUp() || interfaz.isLoopback()) {
                continue;
            }
            Enumeration<InetAddress> direcciones = interfaz.getInetAddresses();
            while (direcciones.hasMoreElements()) {
                InetAddress direccion = direcciones.nextElement();
                if (direccion instanceof Inet4Address && !direccion.isLoopbackAddress()) {
                    return direccion.getHostAddress();
                }
            }
        }
        return InetAddress.getLocalHost().getHostAddress();
    }

    private static void mantenerServidorActivo() throws InterruptedException {
        System.out.println("Servidor activo. Use Ctrl + C para detenerlo.");
        Thread.currentThread().join();
    }
}
