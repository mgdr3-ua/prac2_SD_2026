
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class ConsumidorKafka implements Runnable {

    private final String broker;
    private final String idEstacion;
    private final MotorRiego motor;

    // Constructor
    public ConsumidorKafka(String broker, String idEstacion,
                           MotorRiego motor) {

        this.broker = broker;
        this.idEstacion = idEstacion;
        this.motor = motor;
    }

    @Override
    public void run() {

        Properties props = new Properties();

        // Direccion del broker Kafka
        props.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                broker
        );

        // Cada estacion tiene su propio grupo
        props.put(
                ConsumerConfig.GROUP_ID_CONFIG,
                "engine-" + idEstacion
        );

        // Los mensajes se reciben como cadenas de texto
        props.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName()
        );

        props.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName()
        );

        props.put(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "latest"
        );

        props.put(
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                false
        );

        try (KafkaConsumer<String, String> consumer =
                     new KafkaConsumer<>(props)) {

            // Topic del que recibimos las ordenes
            consumer.subscribe(
                    Collections.singletonList("ordenes-estaciones")
            );

            System.out.println("Consumidor Kafka iniciado.");
            System.out.println("Broker: " + broker);
            System.out.println(
                    "Escuchando ordenes para: " + idEstacion
            );

            // Esperamos ordenes continuamente
            while (!Thread.currentThread().isInterrupted()) {

                ConsumerRecords<String, String> mensajes =
                        consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> mensaje : mensajes) {

                    String contenido = mensaje.value();

                    // Separamos los campos del mensaje
                    String[] partes = contenido.split("#", -1);

                    if (partes.length < 3) {

                        System.out.println(
                                "Orden incorrecta: " + contenido
                        );
                        continue;
                    }

                    // Ignoramos las ordenes de otras estaciones
                    if (!partes[2].equals(idEstacion)) {
                        continue;
                    }

                    System.out.println(
                            "Orden Kafka recibida: " + contenido
                    );

                    try {

                        switch (partes[0]) {

                            case "INICIAR":

                                // INICIAR#solicitud#estacion#operario#duracion
                                if (partes.length != 5) {
                                    System.out.println(
                                            "Formato INICIAR incorrecto."
                                    );
                                    break;
                                }

                                String idSolicitud = partes[1];
                                String idOperario = partes[3];
                                int duracion =
                                        Integer.parseInt(partes[4]);

                                motor.iniciar(
                                        idSolicitud,
                                        idOperario,
                                        duracion
                                );

                                break;

                            case "PARAR":

                                // PARAR#solicitud#estacion
                                if (partes.length != 3) {
                                    System.out.println(
                                            "Formato PARAR incorrecto."
                                    );
                                    break;
                                }

                                motor.parar(partes[1]);

                                break;

                            case "BLOQUEAR":

                                // BLOQUEAR#administracion#estacion
                                if (partes.length != 3) {
                                    System.out.println(
                                            "Formato BLOQUEAR incorrecto."
                                    );
                                    break;
                                }

                                motor.bloquear(partes[1]);

                                break;

                            case "ACTIVAR":

                                // ACTIVAR#administracion#estacion
                                if (partes.length != 3) {
                                    System.out.println(
                                            "Formato ACTIVAR incorrecto."
                                    );
                                    break;
                                }

                                motor.activar(partes[1]);

                                break;

                            default:

                                System.out.println(
                                        "Orden desconocida: " + partes[0]
                                );

                                break;
                        }

                    } catch (NumberFormatException e) {

                        System.out.println(
                                "ERROR: duracion incorrecta en " + contenido
                        );

                    } catch (Exception e) {

                        System.err.println(
                                "ERROR al procesar orden: " + e.getMessage()
                        );
                    }
                }

                // Confirmamos los mensajes procesados
                if (!mensajes.isEmpty()) {
                    consumer.commitSync();
                }
            }

        } catch (Exception e) {

            System.err.println("ERROR en consumidor Kafka:");
            e.printStackTrace();
        }
    }
}
