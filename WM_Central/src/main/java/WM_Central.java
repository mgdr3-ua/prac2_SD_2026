import java.io.IOException; //gestionar errores de entrada o salida
import java.net.ServerSocket; //crear el servidor que escucha conexiones entrantes en un puerto
import java.net.Socket; //conexión entre el servidor y un cliente
import java.sql.SQLException; //gestionar errores relacionados con la base de datos SQLite

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

        // Inicializamos SQLite antes de aceptar conexiones de clientes.
        try {
            BaseDatos.crearTablaEstaciones(); // Preparamos la base de datos.
            BaseDatos.desconectarEstaciones(); // Al reiniciar CENTRAL, las estaciones todavía no están conectadas.
            
            System.out.println("Base de datos SQLite inicializada correctamente.");
        } catch (SQLException e) {
    
            // Si la base de datos falla, mostramos el error.
            System.err.println("Error al inicializar SQLite: " + e.getMessage());

            // Detenemos el programa para evitar trabajar sin base de datos.
            return;
        }

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