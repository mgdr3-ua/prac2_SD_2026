import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class BaseDatos {

    // Dirección del archivo de la base de datos SQLite.
    private static final String URL = "jdbc:sqlite:wm_central.db";

    // Establece una conexión con SQLite.
    public static Connection conectar() throws SQLException {
        return DriverManager.getConnection(URL);
    }

    // Crea la tabla de estaciones si todavía no existe.
    public static void crearTablaEstaciones() throws SQLException {

        String sql = """
            CREATE TABLE IF NOT EXISTS estaciones (
            id TEXT PRIMARY KEY,
            ubicacion TEXT NOT NULL,
            estado TEXT NOT NULL DEFAULT 'DISPONIBLE',
            bloqueada INTEGER NOT NULL DEFAULT 0
            )
            """;

        // Abrimos la conexión y ejecutamos la sentencia SQL.
        // Los recursos se cierran automáticamente al terminar.
        try (Connection conexion = conectar();
             Statement sentencia = conexion.createStatement()) {

            sentencia.executeUpdate(sql);

            // Comprobamos si la tabla existente ya tiene la columna estado.
            boolean tieneEstado = false;

            try (java.sql.ResultSet columnas = sentencia.executeQuery("PRAGMA table_info(estaciones)")) {

                while (columnas.next()) {
                    if ("estado".equalsIgnoreCase(columnas.getString("name"))) {
                        tieneEstado = true;
                        break;
                    }
                }
            }

        // Si la base de datos es antigua, añadimos la columna sin borrar registros.
        if (!tieneEstado) {
            sentencia.executeUpdate("ALTER TABLE estaciones ADD COLUMN estado TEXT NOT NULL DEFAULT 'DISPONIBLE'");
            System.out.println("Columna estado añadida a la tabla estaciones.");
        }

            boolean tieneBloqueada = false;
            try (ResultSet columnas = sentencia.executeQuery("PRAGMA table_info(estaciones)")) {
                while (columnas.next()) {
                    if ("bloqueada".equalsIgnoreCase(columnas.getString("name"))) tieneBloqueada = true;
                }
            }
            if (!tieneBloqueada) {
                sentencia.executeUpdate("ALTER TABLE estaciones ADD COLUMN bloqueada INTEGER NOT NULL DEFAULT 0");
            }
            // Conservar bloqueos previos cuando se migra una base de datos antigua.
            sentencia.executeUpdate("UPDATE estaciones SET bloqueada=1 WHERE estado='FUERA_SERVICIO'");
            // Así conservamos las estaciones registradas anteriormente.
            int estacionesActualizadas = sentencia.executeUpdate(
                "UPDATE estaciones SET estado = 'DISPONIBLE' WHERE estado = 'ACTIVA'"
            );

            if (estacionesActualizadas > 0) {
                System.out.println(
                    "Estados antiguos actualizados: " + estacionesActualizadas
                );
            }
            // Actualizamos los estados antiguos al nuevo formato.
            System.out.println("Tabla de estaciones preparada correctamente.");
        }
    }

    // Datos en tiempo real del riego, compartidos por CENTRAL y el panel.
    public static void crearTablaRiegos() throws SQLException {
        String sql = """
            CREATE TABLE IF NOT EXISTS riegos_actuales (
              estacion TEXT PRIMARY KEY,
              operario TEXT NOT NULL,
              caudal REAL NOT NULL DEFAULT 0,
              volumen REAL NOT NULL DEFAULT 0,
              segundos INTEGER NOT NULL DEFAULT 0
            )
            """;
        try (Connection con = conectar(); Statement st = con.createStatement()) {
            st.executeUpdate(sql);
            // No mostrar riegos antiguos después de reiniciar CENTRAL.
            st.executeUpdate("DELETE FROM riegos_actuales");
        }
    }

    public static void iniciarDatosRiego(String est, String op) throws SQLException {
        String sql = "INSERT INTO riegos_actuales(estacion,operario,caudal,volumen,segundos) "
                + "VALUES(?,?,0,0,0) ON CONFLICT(estacion) DO UPDATE SET "
                + "operario=excluded.operario,caudal=0,volumen=0,segundos=0";
        try (Connection con = conectar(); PreparedStatement ps = con.prepareStatement(sql)) {
            ps.setString(1,est); ps.setString(2,op); ps.executeUpdate();
        }
    }

    public static void actualizarDatosRiego(String est, double caudal, double volumen, int segundos)
            throws SQLException {
        String sql = "UPDATE riegos_actuales SET caudal=?,volumen=?,segundos=? WHERE estacion=?";
        try (Connection con = conectar(); PreparedStatement ps = con.prepareStatement(sql)) {
            ps.setDouble(1,caudal); ps.setDouble(2,volumen);
            ps.setInt(3,segundos); ps.setString(4,est); ps.executeUpdate();
        }
    }

    public static void eliminarDatosRiego(String est) throws SQLException {
        try (Connection con = conectar(); PreparedStatement ps = con.prepareStatement(
                "DELETE FROM riegos_actuales WHERE estacion=?")) {
            ps.setString(1,est); ps.executeUpdate();
        }
    }


    // Datos del ultimo progreso conocido para informar de una fuga inmediata.
    public record DatosRiego(int segundos, double volumen) {}

    public static DatosRiego obtenerDatosRiego(String est) throws SQLException {
        try (Connection con = conectar(); PreparedStatement ps = con.prepareStatement(
                "SELECT segundos,volumen FROM riegos_actuales WHERE estacion=?")) {
            ps.setString(1, est);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? new DatosRiego(rs.getInt(1), rs.getDouble(2)) : new DatosRiego(0,0);
            }
        }
    }
    public static void crearTablaOperarios() throws SQLException {

        String sql = "CREATE TABLE IF NOT EXISTS operarios (id TEXT PRIMARY KEY)";

        try (Connection conexion = conectar();
            Statement sentencia = conexion.createStatement()) {

            sentencia.executeUpdate(sql);
            System.out.println("Tabla de operarios preparada correctamente.");
        }
    }

    // Marca como desconectadas las estaciones al iniciar CENTRAL.
    public static void desconectarEstaciones() throws SQLException {

        String sql = "UPDATE estaciones SET estado = 'DESCONECTADA'";

        try (Connection conexion = conectar();
            Statement sentencia = conexion.createStatement()) {

            int cantidad = sentencia.executeUpdate(sql);

            System.out.println(
                "Estaciones marcadas como desconectadas: " + cantidad
            );
        }
    }
    // Guarda una estación en la base de datos SQLite.
    // Si la estación ya existe, actualiza su ubicación.
    public static void guardarEstacion(String id, String ubicacion) throws SQLException {

        // Consulta SQL para insertar o actualizar una estación.
        String sql = """
            INSERT INTO estaciones (id, ubicacion, estado)
            VALUES (?, ?, 'DISPONIBLE')
            ON CONFLICT(id) DO UPDATE SET
                ubicacion = excluded.ubicacion,
                estado = CASE WHEN estaciones.estado = 'DESCONECTADA' AND estaciones.bloqueada=0 THEN 'DISPONIBLE' WHEN estaciones.estado = 'DESCONECTADA' THEN 'FUERA_SERVICIO' ELSE estaciones.estado END
            """;

        // Abrimos la conexión con SQLite.
        // PreparedStatement permite ejecutar consultas con parámetros.
        try (Connection conexion = conectar();
             java.sql.PreparedStatement sentencia = conexion.prepareStatement(sql)) {

            // Asignamos los valores a los parámetros de la consulta.
            sentencia.setString(1, id);
            sentencia.setString(2, ubicacion);

            // Ejecutamos la operación en la base de datos.
            sentencia.executeUpdate();

            // Mostramos un mensaje para confirmar la operación.
            System.out.println("Estacion guardada en SQLite: " + id);
        }
    }

    
    // TRANSICIONES: la fuga prevalece; ningun OK puede anular un bloqueo/fuga.
    public static boolean actualizarEstado(String id, String estado) throws SQLException {
        String sql;
        switch (estado) {
            case "FUGA" -> sql = "UPDATE estaciones SET estado='FUGA' WHERE id=?";
            case "DESCONECTADA" -> sql = "UPDATE estaciones SET estado='DESCONECTADA' WHERE id=?";
            case "DISPONIBLE" -> sql = "UPDATE estaciones SET estado=CASE WHEN bloqueada=1 THEN 'FUERA_SERVICIO' "
                    + "ELSE 'DISPONIBLE' END WHERE id=? AND estado='DESCONECTADA'";
            default -> sql = "UPDATE estaciones SET estado=? WHERE id=? AND estado!='FUGA'";
        }
        try (Connection con = conectar(); PreparedStatement ps = con.prepareStatement(sql)) {
            if (!estado.equals("FUGA") && !estado.equals("DESCONECTADA") && !estado.equals("DISPONIBLE")) {
                ps.setString(1, estado);
                ps.setString(2, id);
            } else {
                ps.setString(1, id);
            }
            return ps.executeUpdate() > 0;
        }
    }

    // Actualiza el estado solo si coincide con el anterior esperado.
    public static boolean cambiarEstadoSi(String id, String anterior, String nuevo)
            throws SQLException {
        String sql = "UPDATE estaciones SET estado = ? WHERE id = ? AND estado = ?";
        try (Connection conexion = conectar();
             PreparedStatement sentencia = conexion.prepareStatement(sql)) {
            sentencia.setString(1, nuevo);
            sentencia.setString(2, id);
            sentencia.setString(3, anterior);
            return sentencia.executeUpdate() > 0;
        }
    }

    // El bloqueo administrativo persiste aunque la estacion se desconecte o tenga una fuga.
    public static boolean bloquearEstacion(String id) throws SQLException {
        String sql = "UPDATE estaciones SET bloqueada=1, "
                + "estado=CASE WHEN estado IN ('REGANDO','DISPONIBLE') THEN 'FUERA_SERVICIO' "
                + "ELSE estado END WHERE id=? AND bloqueada=0";
        try (Connection con = conectar(); PreparedStatement ps = con.prepareStatement(sql)) {
            ps.setString(1, id);
            return ps.executeUpdate() > 0;
        }
    }

    public static boolean activarEstacion(String id) throws SQLException {
        String sql = "UPDATE estaciones SET bloqueada=0, "
                + "estado=CASE WHEN estado='FUERA_SERVICIO' THEN 'DISPONIBLE' ELSE estado END "
                + "WHERE id=? AND bloqueada=1 AND estado='FUERA_SERVICIO'";
        try (Connection con = conectar(); PreparedStatement ps = con.prepareStatement(sql)) {
            ps.setString(1, id);
            return ps.executeUpdate() > 0;
        }
    }

    public static java.util.List<String> listarIdsEstaciones() throws SQLException {
        java.util.List<String> ids = new java.util.ArrayList<>();
        try (Connection con = conectar(); Statement st = con.createStatement();
                ResultSet rs = st.executeQuery("SELECT id FROM estaciones ORDER BY id")) {
            while (rs.next()) ids.add(rs.getString(1));
        }
        return ids;
    }

    // Respuesta compacta para la consulta de WM_FO mediante Kafka.
    public static String listarDisponibles() throws SQLException {
        StringBuilder sb = new StringBuilder();
        try (Connection con = conectar(); Statement st = con.createStatement();
                ResultSet rs = st.executeQuery("SELECT id,ubicacion FROM estaciones "
                        + "WHERE estado='DISPONIBLE' AND bloqueada=0 ORDER BY id")) {
            while (rs.next()) {
                if (sb.length() > 0) sb.append(';');
                sb.append(rs.getString(1)).append(" (")
                  .append(rs.getString(2).replace('#', ' ')).append(')');
            }
        }
        return sb.length() == 0 ? "NINGUNA" : sb.toString();
    }

    // Comprueba si una estación está disponible para iniciar un riego.
    public static boolean estacionDisponible(String id) throws SQLException {

        String sql = "SELECT estado FROM estaciones WHERE id = ?";

        try (Connection conexion = conectar();
            PreparedStatement sentencia = conexion.prepareStatement(sql)) {

            sentencia.setString(1, id);

            try (ResultSet resultado = sentencia.executeQuery()) {

                // Solo está disponible si existe y su estado es DISPONIBLE.
                return resultado.next()
                        && "DISPONIBLE".equals(resultado.getString("estado"));
            }
        }
    }

    // Permite volver a DISPONIBLE cuando se resuelve una fuga.
    public static boolean resolverAveria(String id) throws SQLException {

        String sql = "UPDATE estaciones SET estado = CASE WHEN bloqueada=1 THEN 'FUERA_SERVICIO' "
                + "ELSE 'DISPONIBLE' END WHERE id = ? AND estado = 'FUGA'";

        try (Connection conexion = conectar();
            PreparedStatement sentencia = conexion.prepareStatement(sql)) {

            sentencia.setString(1, id);

            // Devuelve true si se ha actualizado la estación.
            return sentencia.executeUpdate() > 0;
        }
    }

    // Comprueba si un operario está registrado en la base de datos.
    // Devuelve true si existe y false si no está registrado.
    public static boolean operarioRegistrado(String idOperario) throws SQLException {

        // Buscamos un operario que tenga el identificador recibido.
        // El ? se sustituirá por el ID para evitar introducirlo directamente en SQL.
        String sql = "SELECT 1 FROM operarios WHERE id = ?";

        // Abrimos la conexión y preparamos la consulta.
        // Los recursos se cierran automáticamente al terminar.
        try (Connection conexion = conectar();
            PreparedStatement sentencia = conexion.prepareStatement(sql)) {

            // Sustituimos el ? por el identificador del operario.
            sentencia.setString(1, idOperario);

            // Ejecutamos la consulta y comprobamos si devuelve algún resultado.
            try (ResultSet resultado = sentencia.executeQuery()) {

                // Si existe una fila, el operario está registrado.
                return resultado.next();
            }
        }
    }

    // Registra un operario en la base de datos utilizando su identificador.
    public static void registrarOperario(String idOperario) throws SQLException {

        // Evitamos registrar dos veces al mismo operario.
        String sql = "INSERT OR IGNORE INTO operarios (id) VALUES (?)";

        // Abrimos la conexión y preparamos la sentencia SQL.
        try (Connection conexion = conectar();
            PreparedStatement sentencia = conexion.prepareStatement(sql)) {

            // Sustituimos el ? por el identificador del operario.
            sentencia.setString(1, idOperario);

            // Ejecutamos la inserción en la base de datos.
            sentencia.executeUpdate();
        }
    }

    // Consulta el estado de una estacion.
    // Devuelve null si la estacion no existe.
    public static String obtenerEstadoEstacion(String id) throws SQLException {

        String sql = "SELECT estado FROM estaciones WHERE id = ?";

        try (Connection conexion = conectar();
            PreparedStatement sentencia = conexion.prepareStatement(sql)) {

            sentencia.setString(1, id);

            try (ResultSet resultado = sentencia.executeQuery()) {
                if (resultado.next()) {
                    return resultado.getString("estado");
                }
            }
        }

        return null;
    }
}