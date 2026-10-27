import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class ConsumidorEventosKafka extends Thread {

    private final String direccionKafka;

    public ConsumidorEventosKafka(String direccionKafka) {
        this.direccionKafka = direccionKafka;
    }

    @Override
    public void run() {

        Properties propiedades = new Properties();

        // Direccion del broker Kafka
        propiedades.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, direccionKafka);

        // Grupo independiente para los eventos del motor
        propiedades.put(ConsumerConfig.GROUP_ID_CONFIG, "central-eventos");

        // Recibimos los mensajes como texto
        propiedades.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        propiedades.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());

        // Durante las primeras pruebas escuchamos mensajes nuevos
        propiedades.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        try (KafkaConsumer<String, String> consumidor =
                new KafkaConsumer<>(propiedades)) {

            consumidor.subscribe(Collections.singletonList("eventos-estaciones"));

            while (!Thread.currentThread().isInterrupted()) {

                var mensajes = consumidor.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> mensaje : mensajes) {
                    System.out.println("[KAFKA] Evento del motor: " + mensaje.value());
                }
            }

        } catch (Exception e) {
            System.err.println("[KAFKA] Error al recibir eventos: " + e.getMessage());
        }
    }
}