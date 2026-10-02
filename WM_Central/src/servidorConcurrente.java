import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

// servidor concurrente: atender peticiones de simultaneos clientes
// Este codigo se encargara de escuchar conexiones y crea un hilo para cada cliente

public class servidorConcurrente {

    public static void main(String[] args) {

        // Comprobamos que el usuario haya indicado un puerto
        if (args.length != 1) {
            System.out.println("Uso: java ServidorConcurrente <puerto>");
            return;
        }

        int puerto;

        try {
            puerto = Integer.parseInt(args[0]); //gestionar conexiones 
        } catch (NumberFormatException e) {
            System.out.println("ERROR: el puerto debe ser un numero.");
            return;
        }

        // Creamos el ServerSocket: este objeto escucha conexiones en el puerto indicado.
        try (ServerSocket servidor = new ServerSocket(puerto)) {
            System.out.println("==================================");
            System.out.println(" SERVIDOR DE NUMEROS ROMANOS");
            System.out.println("==================================");
            System.out.println("Escuchando en el puerto " + puerto);
            System.out.println("Esperando clientes...");
            
            // el servidor permanece siempre activo para atender a varios clientes
            while (true) {
                Socket socketCliente = servidor.accept(); //accept() se queda esperando hasta que un cliente se conecta

                System.out.println();
                System.out.println("Nuevo cliente conectado: " + socketCliente.getInetAddress() + ":" + socketCliente.getPort());

                hiloServidor hilo = new hiloServidor(socketCliente); //creamos un hilo para atender a este cliente

                hilo.start(); //arrancamos el hilo
            }
        } catch (IOException e) {
            System.out.println("ERROR en el servidor: " + e.getMessage());
        }
    }
}