
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class WM_FO {

    private final String broker;
    private final String idOperario;

    private final KafkaProducer<String, String> productor;

    // Guardamos las solicitudes pendientes de terminar
    private final ConcurrentHashMap<String, CompletableFuture<String>>
            pendientes = new ConcurrentHashMap<>();

    private final CountDownLatch consumidorPreparado =
            new CountDownLatch(1);

    public WM_FO(String broker, String idOperario) {

        this.broker = broker;
        this.idOperario = idOperario;

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        productor = new KafkaProducer<>(props);
    }

    // Escuchamos las notificaciones dirigidas a este operario
    public void escucharNotificaciones() {

        Thread hilo = new Thread(() -> {

            Properties props = new Properties();

            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
            props.put(ConsumerConfig.GROUP_ID_CONFIG,
                    "operario-" + idOperario);

            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class.getName());

            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class.getName());

            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

            try (KafkaConsumer<String, String> consumidor =
                    new KafkaConsumer<>(props)) {

                consumidor.subscribe(
                        List.of("notificaciones-operarios")
                );

                System.out.println(
                        "[FO] Escuchando notificaciones para " + idOperario
                );

                while (!Thread.currentThread().isInterrupted()) {

                    var mensajes = consumidor.poll(Duration.ofMillis(500));

                    // Ya tenemos particiones asignadas y podemos solicitar
                    if (!consumidor.assignment().isEmpty()) {
                        consumidorPreparado.countDown();
                    }

                    for (ConsumerRecord<String, String> registro : mensajes) {

                        String[] campos = registro.value().split("#", -1);

                        // Formato: TIPO#SOLICITUD#OPERARIO#ESTACION...
                        if (campos.length < 4) {
                            continue;
                        }

                        if (!campos[2].equals(idOperario)) {
                            continue;
                        }

                        String tipo = campos[0];
                        String idSolicitud = campos[1];

                        System.out.println(
                                "[FO] Notificacion: " + registro.value()
                        );

                        // Una solicitud acaba cuando termina o es rechazada
                        if (tipo.equals("FINALIZADA")
                                || tipo.equals("RECHAZADA")) {

                            CompletableFuture<String> pendiente =
                                    pendientes.remove(idSolicitud);

                            if (pendiente != null) {
                                pendiente.complete(registro.value());
                            }
                        }
                    }

                    if (!mensajes.isEmpty()) {
                        consumidor.commitSync();
                    }
                }

            } catch (Exception e) {
                System.err.println(
                        "[FO] Error en consumidor: " + e.getMessage()
                );
                consumidorPreparado.countDown();
            }
        });

        hilo.setDaemon(true);
        hilo.setName("Notificaciones-" + idOperario);
        hilo.start();
    }

    // Solicitamos un riego a CENTRAL a traves de Kafka
    public CompletableFuture<String> solicitarRiego(
            String idEstacion, int duracion) throws Exception {

        if (idEstacion.isBlank() || duracion <= 0) {
            throw new IllegalArgumentException(
                    "Estacion o duracion incorrecta"
            );
        }

        String idSolicitud = "SOL-" +
                UUID.randomUUID().toString().substring(0, 8);

        String mensaje = "SOLICITAR#" + idSolicitud + "#"
                + idOperario + "#" + idEstacion + "#" + duracion;

        CompletableFuture<String> resultado =
                new CompletableFuture<>();

        pendientes.put(idSolicitud, resultado);

        try {
            productor.send(
                    new ProducerRecord<>(
                            "solicitudes-riego", idOperario, mensaje
                    )
            ).get(10, TimeUnit.SECONDS);

            System.out.println("[FO] Solicitud enviada: " + mensaje);

        } catch (Exception e) {
            pendientes.remove(idSolicitud);
            throw e;
        }

        return resultado;
    }

    // Ejecutamos las solicitudes de un fichero secuencialmente
    public void ejecutarFichero(String nombreFichero) throws Exception {

        List<String> lineas = Files.readAllLines(
                Path.of(nombreFichero)
        );

        for (String linea : lineas) {

            linea = linea.trim();

            if (linea.isEmpty() || linea.startsWith("#")) {
                continue;
            }

            String[] datos = linea.split("#", -1);

            if (datos.length != 2) {
                System.out.println("[FO] Linea incorrecta: " + linea);
                continue;
            }

            try {
                String estacion = datos[0].trim();
                int segundos = Integer.parseInt(datos[1].trim());

                CompletableFuture<String> resultado =
                        solicitarRiego(estacion, segundos);

                // Esperamos a que CENTRAL notifique el resultado final
                try {
                    String respuesta = resultado.get(
                            300, TimeUnit.SECONDS
                    );

                    System.out.println(
                            "[FO] Solicitud completada: " + respuesta
                    );

                } catch (java.util.concurrent.TimeoutException e) {
                    System.out.println(
                            "[FO] Tiempo de espera agotado para " + estacion
                    );
                }

                // El enunciado exige 4 segundos entre servicios
                System.out.println(
                        "[FO] Esperando 4 segundos..."
                );
                Thread.sleep(4000);

            } catch (NumberFormatException e) {
                System.out.println(
                        "[FO] Duracion incorrecta: " + linea
                );
            }
        }

        System.out.println("[FO] Fichero procesado.");
    }

    public void menu() {

        Scanner teclado = new Scanner(System.in);

        while (true) {

            System.out.println("\n=== WM_FO ===");
            System.out.println("1. Solicitar riego");
            System.out.println("2. Leer solicitudes de fichero");
            System.out.println("0. Salir");
            System.out.print("Opcion: ");

            String opcion = teclado.nextLine().trim();

            try {
                switch (opcion) {

                    case "1":
                        System.out.print("ID de estacion: ");
                        String estacion = teclado.nextLine().trim();

                        System.out.print("Duracion (segundos): ");
                        int duracion = Integer.parseInt(
                                teclado.nextLine().trim()
                        );

                        solicitarRiego(estacion, duracion);
                        break;

                    case "2":
                        System.out.print(
                                "Ruta del fichero (ej. riegos.txt): "
                        );
                        String fichero = teclado.nextLine().trim();

                        ejecutarFichero(fichero);
                        break;

                    case "0":
                        productor.close();
                        return;

                    default:
                        System.out.println("Opcion incorrecta.");
                }

            } catch (Exception e) {
                System.out.println(
                        "[FO] Error: " + e.getMessage()
                );
            }
        }
    }

    public static void main(String[] args) {

        if (args.length != 2) {
            System.out.println(
                    "Uso: WM_FO <broker:puerto> <idOperario>"
            );
            return;
        }

        String broker = args[0];
        String idOperario = args[1];

        WM_FO operario = new WM_FO(broker, idOperario);

        operario.escucharNotificaciones();

        try {
            // Evitamos enviar solicitudes antes de que Kafka escuche
            if (!operario.consumidorPreparado.await(
                    50, TimeUnit.SECONDS)) {
                System.out.println(
                        "[FO] No se pudo preparar el consumidor Kafka."
                );
                operario.productor.close();
                return;
            }

            System.out.println(
                    "[FO] Operario iniciado: " + idOperario
            );

            operario.menu();

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            operario.productor.close();
        }
    }
}
