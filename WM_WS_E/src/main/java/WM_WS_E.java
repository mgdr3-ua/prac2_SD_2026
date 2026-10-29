
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

        // Datos de conexion del monitor
        String ipMonitor = "localhost";
        int puertoMonitor = 5001;

        // Variable compartida para detectar averias
        AtomicBoolean averia = new AtomicBoolean(false);

        // Creamos el motor encargado del riego
        MotorRiego motor = new MotorRiego(
                brokerKafka,
                idEstacion,
                averia
        );

        // Iniciamos el consumidor Kafka en otro hilo
        Thread hiloKafka = new Thread(
                new ConsumidorKafka(
                        brokerKafka,
                        idEstacion,
                        motor
                )
        );

        hiloKafka.setName("Kafka-Consumer-" + idEstacion);
        hiloKafka.setDaemon(true);
        hiloKafka.start();

        System.out.println("Hilo consumidor Kafka iniciado.");

        // Conexion TCP con el monitor
        try (Socket socket = new Socket(ipMonitor, puertoMonitor)) {

            System.out.println(
                    "Conexion establecida con WM_WS_M."
            );

            System.out.println(
                    "Monitor: " + ipMonitor + ":" + puertoMonitor
            );

            // Flujos de comunicacion TCP
            DataInputStream entrada =
                    new DataInputStream(socket.getInputStream());

            DataOutputStream salida =
                    new DataOutputStream(socket.getOutputStream());

            // Hilo para leer las teclas del usuario
            Thread teclado = new Thread(() -> {

                Scanner scanner = new Scanner(System.in);

                System.out.println(
                        "Pulsa F + Enter para simular una fuga."
                );

                System.out.println(
                        "Pulsa R + Enter para resolver la fuga."
                );

                System.out.println(
                        "Pulsa P + Enter para parar el riego."
                );

                while (scanner.hasNextLine()) {

                    String opcion = scanner.nextLine();

                    if (opcion.equalsIgnoreCase("F")) {

                        // Activamos la simulacion de fuga
                        averia.set(true);

                        System.out.println(
                                "AVERIA SIMULADA: Fuga detectada."
                        );

                    } else if (opcion.equalsIgnoreCase("R")) {

                        // Resolvemos la fuga
                        averia.set(false);

                        System.out.println(
                                "RECUPERACION: Fuga resuelta."
                        );

                    } else if (opcion.equalsIgnoreCase("P")) {

                        // Paramos el riego actual
                        motor.pararActual();

                    } else {

                        System.out.println(
                                "Opcion desconocida. Usa F, R o P."
                        );
                    }
                }
            });

            teclado.setDaemon(true);
            teclado.start();

            // Atendemos las comprobaciones del monitor
            while (true) {

                String mensaje = entrada.readUTF();

                System.out.println(
                        "Mensaje recibido del Monitor: " + mensaje
                );

                if (mensaje.equals("PING")) {

                    if (averia.get()) {

                        // En caso de fuga respondemos KO
                        salida.writeUTF("KO");
                        salida.flush();

                        System.out.println(
                                "Respuesta enviada: KO"
                        );

                    } else {

                        // Si funciona bien respondemos OK
                        salida.writeUTF("OK");
                        salida.flush();

                        System.out.println(
                                "Respuesta enviada: OK"
                        );
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

            System.out.println(
                    "Motivo: " + e.getMessage()
            );
        }
    }
}
