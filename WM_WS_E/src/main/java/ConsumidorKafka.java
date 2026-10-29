
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

    public ConsumidorKafka(String broker, String idEstacion) {
        this.broker = broker;
        this.idEstacion = idEstacion;
    }

    @Override
    public void run() {

        // Configuracion del consumidor Kafka
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);

        // Cada estacion tiene su propio grupo de consumidores
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "engine-" + idEstacion);

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());

        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());

        // En una primera ejecucion, leer solo mensajes nuevos
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        try (KafkaConsumer<String, String> consumer =
                     new KafkaConsumer<>(props)) {

            // Escuchamos las ordenes enviadas desde CENTRAL
            consumer.subscribe(Collections.singletonList("ordenes-estaciones"));

            System.out.println("Consumidor Kafka iniciado.");
            System.out.println("Broker: " + broker);
            System.out.println("Escuchando ordenes para: " + idEstacion);

            while (!Thread.currentThread().isInterrupted()) {

                ConsumerRecords<String, String> mensajes =
                        consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> mensaje : mensajes) {

                    String contenido = mensaje.value();
                    String[] partes = contenido.split("#", -1);

                    // Todas las ordenes tienen el ID de estacion
                    // en la tercera posicion
                    if (partes.length < 3) {
                        System.out.println("Orden mal formada: " + contenido);
                        continue;
                    }

                    if (!partes[2].equals(idEstacion)) {
                        continue;
                    }

                    System.out.println("Orden Kafka recibida: " + contenido);

                    // De momento solo mostramos la orden.
                    // Despues implementaremos INICIAR, PARAR,
                    // BLOQUEAR y ACTIVAR.
                }

                // Confirmamos los mensajes leidos durante la prueba
                if (!mensajes.isEmpty()) {
                    consumer.commitSync();
                }
            }

        } catch (Exception e) {
    System.out.println("ERROR en consumidor Kafka:");
    e.printStackTrace();
}
    }
}
