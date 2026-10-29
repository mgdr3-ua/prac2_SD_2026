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

    // Caudal simulado en litros por minuto
    private final double caudal = 12.0;

    public MotorRiego(String broker, String idEstacion,
                      AtomicBoolean averia) {

        this.idEstacion = idEstacion;
        this.averia = averia;

        Properties props = new Properties();

        props.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                broker
        );

        props.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName()
        );

        props.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName()
        );

        props.put(ProducerConfig.ACKS_CONFIG, "all");

        productor = new KafkaProducer<>(props);
    }

    // Publica los eventos del motor en Kafka
    private void publicar(String mensaje) {

        productor.send(
                new ProducerRecord<>(
                        "eventos-estaciones",
                        idEstacion,
                        mensaje
                ),
                (metadata, error) -> {
                    if (error != null) {
                        System.err.println(
                                "Error Kafka: " + error.getMessage()
                        );
                    }
                }
        );

        System.out.println("[RIEGO] " + mensaje);
    }

    // Inicia el riego si la estacion esta disponible
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

        // Rechazamos el riego si hay algun problema
        if (motivo != null) {

            publicar(
                    "RECHAZADO#" + idSolicitud + "#"
                    + idEstacion + "#" + motivo
            );

            return;
        }

        regando = true;
        detener = false;
        solicitudActual = idSolicitud;

        // Ejecutamos el riego en otro hilo
        Thread hiloRiego = new Thread(
                () -> ejecutarRiego(
                        idSolicitud,
                        idOperario,
                        duracion
                )
        );

        hiloRiego.setName("Riego-" + idEstacion);
        hiloRiego.start();
    }

    // Simula el riego segundo a segundo
    private void ejecutarRiego(String idSolicitud,
                               String idOperario,
                               int duracion) {

        int segundos = 0;
        double volumen = 0.0;
        String motivo = "TIEMPO";

        // Avisamos de que empieza el riego
        publicar(
                "INICIADO#" + idSolicitud + "#"
                + idEstacion + "#" + idOperario
        );

        try {

            while (segundos < duracion) {

                // Comprobamos si existe una fuga
                if (averia.get()) {
                    motivo = "FUGA";
                    break;
                }

                // Comprobamos si la estacion esta bloqueada
                if (bloqueada) {
                    motivo = "BLOQUEO";
                    break;
                }

                // Comprobamos si se ha solicitado parar
                if (detener) {
                    motivo = "MANUAL";
                    break;
                }

                Thread.sleep(1000);

                // Si ha ocurrido algo durante la espera,
                // evitamos contar un segundo mas de riego
                if (averia.get() || bloqueada || detener) {
                    continue;
                }

                segundos++;

                // Volumen = caudal * tiempo / 60
                volumen = caudal * segundos / 60.0;

                String progreso = String.format(
                        Locale.US,
                        "PROGRESO#%s#%s#%.2f#%.2f#%d",
                        idSolicitud,
                        idEstacion,
                        caudal,
                        volumen,
                        segundos
                );

                publicar(progreso);
            }

            // Determinamos el motivo final
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

            // Publicamos los datos finales del riego
            String finalizado = String.format(
                    Locale.US,
                    "FINALIZADO#%s#%s#%s#%s#%d#%.2f",
                    idSolicitud,
                    idEstacion,
                    idOperario,
                    motivo,
                    segundos,
                    volumen
            );

            publicar(finalizado);

            // Dejamos el motor preparado para otro riego
            synchronized (this) {
                regando = false;
                solicitudActual = null;
                detener = false;
            }
        }
    }

    // Detiene el riego indicado mediante una orden Kafka
    public synchronized void parar(String idSolicitud) {

        if (regando && idSolicitud.equals(solicitudActual)) {

            detener = true;

            System.out.println(
                    "[RIEGO] Parada solicitada desde Kafka: "
                    + idSolicitud
            );

        } else {

            System.out.println(
                    "[RIEGO] No hay un riego activo con ID: "
                    + idSolicitud
            );
        }
    }

    // NUEVO: detiene manualmente el riego con la tecla P
    public synchronized void pararActual() {

        if (regando) {

            detener = true;

            System.out.println(
                    "[RIEGO] Parada manual solicitada: "
                    + solicitudActual
            );

        } else {

            System.out.println(
                    "[RIEGO] No hay ningun riego activo."
            );
        }
    }

    // Bloquea la estacion e impide nuevos riegos
    public void bloquear(String idAdministracion) {

        bloqueada = true;

        publicar(
                "BLOQUEADA#" + idAdministracion
                + "#" + idEstacion
        );
    }

    // Activa una estacion si no tiene averias
    public void activar(String idAdministracion) {

        if (!averia.get()) {

            bloqueada = false;

            publicar(
                    "ACTIVADA#" + idAdministracion
                    + "#" + idEstacion
            );

        } else {

            System.out.println(
                    "[RIEGO] No se puede activar: existe una fuga."
            );
        }
    }
}
