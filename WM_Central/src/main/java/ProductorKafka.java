import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProductorKafka implements AutoCloseable {

    private final KafkaProducer<String, String> productor;

    public ProductorKafka(String direccionKafka) {

        Properties propiedades = new Properties();

        // Direccion del broker Kafka.
        propiedades.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, direccionKafka);

        // Enviamos identificadores y mensajes como texto.
        propiedades.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        propiedades.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        // Esperamos confirmacion del broker.
        propiedades.put(ProducerConfig.ACKS_CONFIG, "all");

        productor = new KafkaProducer<>(propiedades);
    }

    // Envia una notificacion al operario.
    public void notificarOperario(String idOperario, String mensaje) {

        ProducerRecord<String, String> registro =
                new ProducerRecord<>("notificaciones-operarios", idOperario, mensaje);

        productor.send(registro, (resultado, error) -> {
            if (error != null) {
                System.err.println("[KAFKA] Error al enviar notificacion: "
                        + error.getMessage());
            } else {
                System.out.println("[KAFKA] Notificacion enviada: " + mensaje);
            }
        });
    }

    @Override
    public void close() {
        productor.close();
    }
}