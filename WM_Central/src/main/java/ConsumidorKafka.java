import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.sql.SQLException;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class ConsumidorKafka extends Thread {

    private final String direccionKafka;

    public ConsumidorKafka(String direccionKafka) {
        this.direccionKafka = direccionKafka;
    }

    @Override
    public void run() {

        Properties propiedades = new Properties();

        // Direccion del broker Kafka
        propiedades.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, direccionKafka);

        // Grupo de consumidores de CENTRAL
        propiedades.put(ConsumerConfig.GROUP_ID_CONFIG, "central-solicitudes");

        // Los mensajes se reciben como texto
        propiedades.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        propiedades.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Para esta primera prueba, leemos mensajes nuevos
        propiedades.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        try (KafkaConsumer<String, String> consumidor = new KafkaConsumer<>(propiedades);
                ProductorKafka productor = new ProductorKafka(direccionKafka)){

            consumidor.subscribe(Collections.singletonList("solicitudes-riego"));

            while (!Thread.currentThread().isInterrupted()) {

                var mensajes = consumidor.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> mensaje : mensajes) {
                    String solicitud = mensaje.value();

                    System.out.println("[KAFKA] Solicitud recibida: " + solicitud);

                    // Separamos los campos utilizando el protocolo acordado.
                    String[] campos = solicitud.split("#", -1);

                    // Una solicitud debe tener exactamente 5 campos.
                    if (campos.length != 5 || !campos[0].equals("SOLICITAR")) {
                        System.out.println("[KAFKA] Formato de solicitud incorrecto.");
                        continue;
                    }

                    String idSolicitud = campos[1];
                    String idOperario = campos[2];
                    String idEstacion = campos[3];

                    int duracion;

                    // Comprobamos que la duracion sea un numero positivo.
                    try {
                        duracion = Integer.parseInt(campos[4]);

                        if (duracion <= 0) {
                            System.out.println("[KAFKA] Duracion no valida.");
                            continue;
                        }
                    } catch (NumberFormatException e) {
                        System.out.println("[KAFKA] Duracion no valida.");
                        continue;
                    }

                    // Comprobamos que los identificadores no esten vacios.
                    if (idSolicitud.isBlank() || idOperario.isBlank() || idEstacion.isBlank()) {
                        System.out.println("[KAFKA] Faltan identificadores en la solicitud.");
                        continue;
                    }

                    // Consultamos SQLite para validar la solicitud.
                    try {
                        if (!BaseDatos.operarioRegistrado(idOperario)) {
                            System.out.println("[KAFKA] Solicitud " + idSolicitud + " rechazada: operario no registrado.");

                            String respuesta = "RECHAZADA#" + idSolicitud + "#" + idOperario + "#" + idEstacion + "#OPERARIO_INVALIDO";

                            productor.notificarOperario(idOperario, respuesta);
                            continue;
                        }

                        if (!BaseDatos.estacionDisponible(idEstacion)) {
                            System.out.println("[KAFKA] Solicitud " + idSolicitud + " rechazada: estacion no disponible.");
                            continue;
                        }

                        System.out.println("[KAFKA] Solicitud " + idSolicitud + " validada correctamente.");
                        System.out.println("Estacion: " + idEstacion);
                        System.out.println("Operario: " + idOperario);
                        System.out.println("Duracion maxima: " + duracion + " segundos.");

                    } catch (SQLException e) {
                        System.err.println("[KAFKA] Error al consultar SQLite: " + e.getMessage());
                    }
                }
            }

        } catch (Exception e) {
            System.err.println("Error en consumidor Kafka: " + e.getMessage());
        }
    }
}