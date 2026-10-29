import java.io.IOException; //gestionar errores de entrada o salida
import java.net.ServerSocket; //crear el servidor que escucha conexiones entrantes en un puerto
import java.net.Socket; //conexión entre el servidor y un cliente
import java.sql.SQLException; //gestionar errores relacionados con la base de datos SQLite

public class WM_Central {

    public static void main(String[] args) {

        System.out.println("=== WATER MANAGEMENT - CENTRAL ===");

        // Comprobamos que se haya indicado el puerto
        if (args.length != 2) {
            System.out.println("Uso: java WM_Central <puerto> <kafka>");
            return;
        }

        int puerto;

        try {
            puerto = Integer.parseInt(args[0]);
        } catch (NumberFormatException e) {
            System.out.println("ERROR: el puerto debe ser un numero.");
            return;
        }
        // Direccion del servidor Kafka
        String kafka = args[1];

        System.out.println("Puerto de escucha: " + puerto);
        System.out.println("Kafka: " + kafka);

        // Inicializamos SQLite antes de aceptar conexiones de clientes.
        try {
            BaseDatos.crearTablaEstaciones(); // Preparamos la base de datos.
            BaseDatos.crearTablaOperarios();
            // Operarios de la demostracion; ampliar con WM_OPERARIOS=OP-01,OP-02,...
            String registrados = System.getenv().getOrDefault("WM_OPERARIOS", "OP-01,OP-02,ADMIN");
            for (String op : registrados.split(",")) {
                if (!op.trim().isEmpty()) BaseDatos.registrarOperario(op.trim());
            }
            BaseDatos.registrarOperario("ADMIN");
            BaseDatos.crearTablaRiegos();
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
        // Iniciamos el menú en otro hilo para poder utilizarlo
        // mientras CENTRAL sigue atendiendo conexiones de las estaciones.
        MenuCentral menu = new MenuCentral(kafka);
        menu.start();
        
        // Iniciamos el consumidor Kafka en otro hilo
        // para recibir solicitudes sin bloquear el servidor TCP.
        ConsumidorKafka consumidorKafka = new ConsumidorKafka(kafka);
        consumidorKafka.start();
        // Escuchamos los eventos enviados por los motores de las estaciones.
        ConsumidorEventosKafka consumidorEventos = new ConsumidorEventosKafka(kafka);
        consumidorEventos.start();

        // Evitar que una peticion sin respuesta del Engine bloquee la estacion para siempre.
        Thread vigilante = new Thread(() -> {
            try (ProductorKafka productor = new ProductorKafka(kafka)) {
                while (!Thread.currentThread().isInterrupted()) {
                    Thread.sleep(10000);
                    for (SolicitudesActivas.Expirada caducada : SolicitudesActivas.expirar(45000)) {
                        System.out.println("[CENTRAL] Sin respuesta/progreso: " + caducada.idSolicitud());
                        if (caducada.iniciada()) {
                            productor.enviarOrdenEstacion(caducada.idEstacion(),
                                    "PARAR#" + caducada.idSolicitud() + "#" + caducada.idEstacion());
                            BaseDatos.actualizarEstado(caducada.idEstacion(), "DESCONECTADA");
                            BaseDatos.eliminarDatosRiego(caducada.idEstacion());
                        }
                        productor.notificarOperario(caducada.idOperario(), "RECHAZADA#"
                                + caducada.idSolicitud() + "#" + caducada.idOperario() + "#"
                                + caducada.idEstacion() + "#SIN_RESPUESTA");
                    }
                }
            } catch (Exception e) {
                System.err.println("[CENTRAL] Vigilante finalizado: " + e.getMessage());
            }
        }, "Vigilante-solicitudes");
        vigilante.setDaemon(true);
        vigilante.start();

        System.out.println("Esperando conexiones...");
        
        while (true) {
             Socket socketCliente = servidor.accept();

            System.out.println("Nueva conexion recibida desde " + socketCliente.getInetAddress() + ":" + socketCliente.getPort());
            
            // Creamos un hilo independiente para atender a este cliente
            HiloCentral hilo = new HiloCentral(socketCliente, kafka);

            // Iniciamos el hilo para que CENTRAL pueda seguir aceptando otros clientes
            hilo.start();
        }

        } catch (IOException e) {
            System.out.println("ERROR en WM_Central: " + e.getMessage());
        }
    }
}