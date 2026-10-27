import java.util.concurrent.ConcurrentHashMap;

public class SolicitudesActivas {

    // Guardamos el ID de cada solicitud junto con el operario que la ha realizado
    private static final ConcurrentHashMap<String, String> solicitudes =
            new ConcurrentHashMap<>();

    // Añadimos una solicitud cuando CENTRAL la valida
    public static void guardar(String idSolicitud, String idOperario) {
        solicitudes.put(idSolicitud, idOperario);
    }

    // Buscamos el operario al que pertenece una solicitud
    public static String obtenerOperario(String idSolicitud) {
        return solicitudes.get(idSolicitud);
    }

    // Eliminamos la solicitud cuando termina el riego
    public static void eliminar(String idSolicitud) {
        solicitudes.remove(idSolicitud);
    }
}