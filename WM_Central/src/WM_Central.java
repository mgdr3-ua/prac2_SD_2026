import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class WM_Central {

    public static void main(String[] args) {

        System.out.println("=== WATER MANAGEMENT - CENTRAL ===");

        // Comprobamos que se haya indicado el puerto
        if (args.length != 1) {
            System.out.println("Uso: java WM_Central <puerto>");
            return;
        }

        int puerto;

        try {
            puerto = Integer.parseInt(args[0]);
        } catch (NumberFormatException e) {
            System.out.println("ERROR: el puerto debe ser un numero.");
            return;
        }

        System.out.println("Puerto de escucha: " + puerto);

        try (ServerSocket servidor = new ServerSocket(puerto)) {

        System.out.println("Servidor iniciado correctamente.");
        System.out.println("Esperando conexiones...");
        
        while (true) {
             Socket socketCliente = servidor.accept();

            System.out.println("Nueva conexion recibida desde " + socketCliente.getInetAddress() + ":" + socketCliente.getPort());
            
            // Creamos un hilo independiente para atender a este cliente
            HiloCentral hilo = new HiloCentral(socketCliente);

            // Iniciamos el hilo para que CENTRAL pueda seguir aceptando otros clientes
            hilo.start();
        }

        } catch (IOException e) {
            System.out.println("ERROR en WM_Central: " + e.getMessage());
        }
    }
}