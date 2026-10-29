
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class MotorRiego {

    private final String idEstacion;
    private final AtomicBoolean averia;
    private final KafkaProducer<String, String> productor;

    private volatile boolean bloqueada = false;
    private volatile boolean detener = false;
    private volatile boolean regando = false;

    private String solicitudActual = null;
    private final double caudal = 12.0; // litros por minuto

    public MotorRiego(String broker, String idEstacion,
                      AtomicBoolean averia) {

        this.idEstacion = idEstacion;
        this.averia = averia;

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                  StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                  StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        productor = new KafkaProducer<>(props);
    }

    private void publicar(String mensaje) {
        productor.send(new ProducerRecord<>(
                "eventos-estaciones", idEstacion, mensaje
        ), (metadata, error) -> {
            if (error != null) {
                System.err.println("Error Kafka: " + error.getMessage());
            }
        });

        System.out.println("[RIEGO] " + mensaje);
    }

    public synchronized void iniciar(String idSolicitud,
                                     String idOperario,
                                     int duracion) {

        String motivo = null;

        if (averia.get()) {
            motivo = "FUGA";
        } else if (bloqueada) {
            motivo = "BLOQUEADA";
        } else if (regando) {
            motivo = "OCUPADA";
        } else if (duracion <= 0) {
            motivo = "ERROR";
        }

        if (motivo != null) {
            publicar("RECHAZADO#" + idSolicitud + "#"
                    + idEstacion + "#" + motivo);
            return;
        }

        regando = true;
        detener = false;
        solicitudActual = idSolicitud;

        Thread hiloRiego = new Thread(() ->
                ejecutarRiego(idSolicitud, idOperario, duracion)
        );

        hiloRiego.setName("Riego-" + idEstacion);
        hiloRiego.start();
    }

    private void ejecutarRiego(String idSolicitud,
                               String idOperario,
                               int duracion) {

        int segundos = 0;
        double volumen = 0.0;
        String motivo = "TIEMPO";

        publicar("INICIADO#" + idSolicitud + "#"
                + idEstacion + "#" + idOperario);

        try {
            while (segundos < duracion) {

                if (averia.get()) {
                    motivo = "FUGA";
                    break;
                }

                if (bloqueada) {
                    motivo = "BLOQUEO";
                    break;
                }

                if (detener) {
                    motivo = "MANUAL";
                    break;
                }

                Thread.sleep(1000);

                if (averia.get() || bloqueada || detener) {
                    continue;
                }

                segundos++;
                volumen = caudal * segundos / 60.0;

                String progreso = String.format(
                    Locale.US,
                    "PROGRESO#%s#%s#%.2f#%.2f#%d",
                    idSolicitud, idEstacion,
                    caudal, volumen, segundos
                );

                publicar(progreso);
            }

            if (averia.get()) {
                motivo = "FUGA";
            } else if (bloqueada) {
                motivo = "BLOQUEO";
            } else if (detener) {
                motivo = "MANUAL";
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            motivo = "MANUAL";
        } finally {

            String finalizado = String.format(
                Locale.US,
                "FINALIZADO#%s#%s#%s#%s#%d#%.2f",
                idSolicitud, idEstacion, idOperario,
                motivo, segundos, volumen
            );

            publicar(finalizado);

            synchronized (this) {
                regando = false;
                solicitudActual = null;
                detener = false;
            }
        }
    }

    public synchronized void parar(String idSolicitud) {
        if (regando && idSolicitud.equals(solicitudActual)) {
            detener = true;
        }
    }

    public void bloquear(String idAdministracion) {
        bloqueada = true;
        publicar("BLOQUEADA#" + idAdministracion
                + "#" + idEstacion);
    }

    public void activar(String idAdministracion) {
        if (!averia.get()) {
            bloqueada = false;
            publicar("ACTIVADA#" + idAdministracion
                    + "#" + idEstacion);
        }
    }
}
