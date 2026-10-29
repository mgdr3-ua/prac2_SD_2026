
import java.util.concurrent.ConcurrentHashMap;

public class SolicitudesActivas {

    // Guardamos el operario y la estacion de cada solicitud
    private static final ConcurrentHashMap<String, Solicitud> solicitudes =
            new ConcurrentHashMap<>();

    // Datos de una solicitud validada
    private static class Solicitud {
        String idOperario;
        String idEstacion;

        Solicitud(String idOperario, String idEstacion) {
            this.idOperario = idOperario;
            this.idEstacion = idEstacion;
        }
    }

    // Guardamos una solicitud si su ID no existe y la estacion esta libre
    public static synchronized boolean guardar(String idSolicitud, String idOperario, String idEstacion) {

        if (solicitudes.containsKey(idSolicitud) || estacionOcupada(idEstacion)) {
            return false;
        }

        solicitudes.put(idSolicitud, new Solicitud(idOperario, idEstacion));
        return true;
    }

    // Obtenemos el operario de una solicitud
    public static String obtenerOperario(String idSolicitud) {
        Solicitud solicitud = solicitudes.get(idSolicitud);
        return solicitud != null ? solicitud.idOperario : null;
    }

    // Comprobamos que la solicitud corresponde al operario y estacion indicados
    public static boolean esValida(String idSolicitud, String idOperario, String idEstacion) {
        Solicitud solicitud = solicitudes.get(idSolicitud);

        return solicitud != null
                && solicitud.idOperario.equals(idOperario)
                && solicitud.idEstacion.equals(idEstacion);
    }
    
    // Comprobamos si una estacion ya tiene una solicitud activa
    public static boolean estacionOcupada(String idEstacion) {
        for (Solicitud solicitud : solicitudes.values()) {
            if (solicitud.idEstacion.equals(idEstacion)) {
                return true;
            }
        }
        return false;
    }

    // Eliminamos la solicitud cuando termina el riego
    public static void eliminar(String idSolicitud) {
        solicitudes.remove(idSolicitud);
    }
}
