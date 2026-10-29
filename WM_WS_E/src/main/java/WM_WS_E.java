
import java.net.Socket;
import java.io.IOException;
import java.io.DataOutputStream;
import java.io.DataInputStream;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicBoolean;

public class WM_WS_E {

    public static void main(String[] args) {

        System.out.println(" --- WATER MANAGER --- ");
        System.out.println("MODULO: Engine de estacion (WM_WS_E)");

        // Configuracion de Kafka y de la estacion
        String brokerKafka = "localhost:9092";
        String idEstacion = "WS-01";

        // Datos de conexion con el monitor
        String ipMonitor = "localhost";
        int puertoMonitor = 5001;

        // Estado compartido de averia
        AtomicBoolean averia = new AtomicBoolean(false);

        // Motor encargado de ejecutar los riegos
        MotorRiego motor = new MotorRiego(
                brokerKafka, idEstacion, averia
        );

        // Hilo que recibe las ordenes desde Kafka
        Thread hiloKafka = new Thread(
                new ConsumidorKafka(brokerKafka, idEstacion, motor)
        );

        hiloKafka.setName("Kafka-Consumer-" + idEstacion);
        hiloKafka.setDaemon(true);
        hiloKafka.start();

        System.out.println("Hilo consumidor Kafka iniciado.");

        // Conexion TCP con el monitor
        try (Socket socket = new Socket(ipMonitor, puertoMonitor)) {

            System.out.println("Conexion establecida con WM_WS_M.");
            System.out.println("Monitor: " + ipMonitor + ":" + puertoMonitor);

            DataInputStream entrada =
                    new DataInputStream(socket.getInputStream());

            DataOutputStream salida =
                    new DataOutputStream(socket.getOutputStream());

            // Hilo independiente para simular y resolver fugas
            Thread teclado = new Thread(() -> {

                Scanner scanner = new Scanner(System.in);

                System.out.println("Pulsa F + Enter para simular una fuga.");
                System.out.println("Pulsa R + Enter para resolver la fuga.");

                while (scanner.hasNextLine()) {

                    String opcion = scanner.nextLine();

                    if (opcion.equalsIgnoreCase("F")) {

                        // Simulamos una averia
                        averia.set(true);

                        System.out.println(
                                "AVERIA SIMULADA: Fuga detectada."
                        );

                    } else if (opcion.equalsIgnoreCase("R")) {

                        // Resolvemos la averia
                        averia.set(false);

                        System.out.println(
                                "RECUPERACION: Fuga resuelta correctamente."
                        );
                    }
                }
            });

            teclado.setDaemon(true);
            teclado.start();

            // Respondemos continuamente a las comprobaciones del monitor
            while (true) {

                String mensaje = entrada.readUTF();

                System.out.println(
                        "Mensaje recibido del Monitor: " + mensaje
                );

                if (mensaje.equals("PING")) {

                    if (averia.get()) {

                        // Hay una fuga
                        salida.writeUTF("KO");
                        salida.flush();

                        System.out.println("Respuesta enviada: KO");

                    } else {

                        // La estacion funciona correctamente
                        salida.writeUTF("OK");
                        salida.flush();

                        System.out.println("Respuesta enviada: OK");
                    }

                } else {

                    System.out.println(
                            "Mensaje desconocido: " + mensaje
                    );
                }
            }

        } catch (IOException e) {

            System.out.println(
                    "ERROR: comunicacion con WM_WS_M."
            );
            System.out.println("Motivo: " + e.getMessage());
        }
    }
}
