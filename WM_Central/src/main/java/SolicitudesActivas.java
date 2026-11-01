
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Gestiona las solicitudes de riego que se encuentran activas en CENTRAL.
 * Permite registrar solicitudes, comprobar si una estación está ocupada,
 * actualizar su actividad y eliminar aquellas que hayan finalizado
 * o superado el tiempo máximo de inactividad.
 *
 * Utiliza ConcurrentHashMap para permitir el acceso concurrente desde
 * distintos hilos del servidor.
 */
public class SolicitudesActivas {
    private static final ConcurrentHashMap<String, Solicitud> solicitudes = new ConcurrentHashMap<>();

    /**
     * Representa la información asociada a una solicitud de riego.
     *
     * Guarda el operario solicitante, la estación de destino, si el riego
     * ha comenzado y el instante de su última actividad.
     */
    private static class Solicitud {
        final String idOperario;
        final String idEstacion;
        boolean iniciada;
        long ultimaActividad = System.currentTimeMillis();

        Solicitud(String op, String est) { idOperario = op; idEstacion = est; }
    }

    /**
     * Representa una solicitud retirada del registro de solicitudes activas.
     * Conserva sus datos para permitir su posterior tratamiento.
     */
    public record Expirada(String idSolicitud, String idOperario, String idEstacion, boolean iniciada) {}

    /**
     * Registra una nueva solicitud si su identificador no existe
     * y la estación correspondiente no tiene otra solicitud activa.
     *
     * El método está sincronizado para evitar registros simultáneos
     * que puedan asignar varias solicitudes a una misma estación.
     *
     * @return true si se registra correctamente; false en caso contrario.
     */
    public static synchronized boolean guardar(String idSolicitud, String idOperario, String idEstacion) {
        if (solicitudes.containsKey(idSolicitud) || estacionOcupada(idEstacion)) return false;
        solicitudes.put(idSolicitud, new Solicitud(idOperario, idEstacion));
        return true;
    }

    /**
     * Obtiene el identificador del operario asociado a una solicitud.
     *
     * @return identificador del operario o null si no existe la solicitud.
     */
    public static String obtenerOperario(String idSolicitud) {
        Solicitud s = solicitudes.get(idSolicitud);
        return s == null ? null : s.idOperario;
    }

    /**
     * Comprueba que una solicitud pertenece al operario y a la estación indicados.
     *
     * @return true si los identificadores coinciden; false en caso contrario.
     */
    public static boolean esValida(String idSolicitud, String idOperario, String idEstacion) {
        Solicitud s = solicitudes.get(idSolicitud);
        return s != null && s.idOperario.equals(idOperario) && s.idEstacion.equals(idEstacion);
    }

    /**
     * Comprueba si una estación tiene alguna solicitud de riego activa.
     *
     * @return true si la estación está ocupada; false si no tiene solicitudes.
     */
    public static boolean estacionOcupada(String idEstacion) {
        for (Solicitud s : solicitudes.values()) {
            if (s.idEstacion.equals(idEstacion)) return true;
        }
        return false;
    }

    /**
     * Actualiza el instante de última actividad de una solicitud.
     * Si se indica que el riego ha comenzado, también marca la solicitud
     * como iniciada.
     */
    public static synchronized void actividad(String idSolicitud, boolean iniciada) {
        Solicitud s = solicitudes.get(idSolicitud);
        if (s != null) {
            s.ultimaActividad = System.currentTimeMillis();
            if (iniciada) s.iniciada = true;
        }
    }

    // Libera solicitudes sin respuesta/progreso durante un periodo excesivo.

    /**
     * Detecta y elimina las solicitudes que superan el tiempo máximo
     * de inactividad establecido.
     *
     * @param inactividadMs tiempo máximo permitido sin actividad, en milisegundos.
     * @return lista de solicitudes expiradas para su posterior tratamiento.
     */
    public static synchronized List<Expirada> expirar(long inactividadMs) {
        long ahora = System.currentTimeMillis();
        List<Expirada> salida = new ArrayList<>();
        solicitudes.forEach((id, s) -> {
            if (ahora - s.ultimaActividad > inactividadMs) {
                salida.add(new Expirada(id, s.idOperario, s.idEstacion, s.iniciada));
                solicitudes.remove(id);
            }
        });
        return salida;
    }


    // La fuga puede llegar antes que FINALIZADO del Engine: cerrar solo una vez.

    /**
     * Busca y elimina la solicitud activa asociada a una estación.
     * Permite retirar una solicitud cuando se produce una incidencia,
     * evitando que permanezca registrada como activa.
     *
     * @return datos de la solicitud retirada o null si no existe.
     */
    public static synchronized Expirada extraerPorEstacion(String idEstacion) {
        for (var entrada : solicitudes.entrySet()) {
            Solicitud s = entrada.getValue();
            if (s.idEstacion.equals(idEstacion)) {
                solicitudes.remove(entrada.getKey());
                return new Expirada(entrada.getKey(), s.idOperario, s.idEstacion, s.iniciada);
            }
        }
        return null;
    }

    /**
     * Elimina una solicitud del registro de solicitudes activas
     * a partir de su identificador.
     */
    public static synchronized void eliminar(String idSolicitud) {
        solicitudes.remove(idSolicitud);
    }
}
