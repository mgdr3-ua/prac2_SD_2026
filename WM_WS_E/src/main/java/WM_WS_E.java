import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicBoolean;

public class WM_WS_E {
    public static void main(String[] args) {
        // Por compatibilidad: sin argumentos funciona en la prueba local antigua.
        if (args.length != 0 && args.length != 3) {
            System.out.println("Uso: WM_WS_E <broker:puerto> <ip_monitor> <puerto_monitor>");
            return;
        }
        String broker = args.length == 3 ? args[0] : "localhost:9092";
        String ipMonitor = args.length == 3 ? args[1] : "localhost";
        int puertoMonitor;
        try {
            puertoMonitor = args.length == 3 ? Integer.parseInt(args[2]) : 5001;
        } catch (NumberFormatException e) {
            System.err.println("Puerto de monitor incorrecto.");
            return;
        }

        try (Socket socket = new Socket(ipMonitor, puertoMonitor)) {
            DataInputStream entrada = new DataInputStream(socket.getInputStream());
            DataOutputStream salida = new DataOutputStream(socket.getOutputStream());
            socket.setSoTimeout(5000);

            // La identidad de la estacion procede del Monitor ya registrado en CENTRAL.
            String bienvenida = entrada.readUTF();
            if (!bienvenida.startsWith("ID#") || bienvenida.substring(3).isBlank()) {
                throw new IOException("El monitor no ha enviado el ID de estacion.");
            }
            String idEstacion = bienvenida.substring(3);
            System.out.println("[ENGINE] Estacion " + idEstacion + " | Kafka " + broker);

            AtomicBoolean averia = new AtomicBoolean(false);
            try (MotorRiego motor = new MotorRiego(broker, idEstacion, averia)) {
                Thread consumidor = new Thread(new ConsumidorKafka(broker, idEstacion, motor),
                        "Kafka-Consumer-" + idEstacion);
                consumidor.setDaemon(true);
                consumidor.start();

                Thread teclado = new Thread(() -> {
                    Scanner scanner = new Scanner(System.in);
                    System.out.println("F: fuga | R: resolver fuga | P: parar riego | I: solicitar riego local");
                    while (scanner.hasNextLine()) {
                        String opcion = scanner.nextLine().trim().toUpperCase();
                        try {
                            switch (opcion) {
                                case "F" -> {
                                    motor.notificarFuga();
                                    System.out.println("[ENGINE] Fuga simulada. Avisando al monitor.");
                                }
                                case "R" -> {
                                    averia.set(false);
                                    System.out.println("[ENGINE] Fuga resuelta.");
                                }
                                case "P" -> motor.pararActual();
                                case "I" -> {
                                    System.out.print("ID de operario registrado: ");
                                    String op = scanner.nextLine().trim();
                                    System.out.print("Duracion (segundos): ");
                                    int duracion = Integer.parseInt(scanner.nextLine().trim());
                                    motor.solicitarRiego(op, duracion);
                                }
                                default -> System.out.println("Opciones: F, R, P, I");
                            }
                        } catch (Exception e) {
                            System.err.println("[ENGINE] Entrada incorrecta: " + e.getMessage());
                        }
                    }
                }, "Teclado-" + idEstacion);
                teclado.setDaemon(true);
                teclado.start();

                // Socket TCP dedicado a salud; el riego usa otros hilos.
                while (true) {
                    String mensaje = entrada.readUTF();
                    if ("PING".equals(mensaje)) {
                        salida.writeUTF(averia.get() ? "KO" : "OK");
                        salida.flush();
                    } else {
                        System.err.println("[ENGINE] Mensaje del monitor desconocido: " + mensaje);
                    }
                }
            }
        } catch (IOException e) {
            System.err.println("[ENGINE] Conexion con monitor perdida: " + e.getMessage());
        }
    }
}
