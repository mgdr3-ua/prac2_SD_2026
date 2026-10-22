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
            estado TEXT NOT NULL DEFAULT 'DISPONIBLE'
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
                ubicacion = excluded.ubicacion
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

    
    // Cambia el estado de una estación que ya está registrada.
    // Devuelve true si encuentra la estación y false si no existe.
    public static boolean actualizarEstado(String id, String estado) throws SQLException {

        String sql = "UPDATE estaciones SET estado = ? WHERE id = ? AND estado != 'FUGA'";

        // Abrimos la conexión y preparamos la consulta SQL.
        try (Connection conexion = conectar();
             java.sql.PreparedStatement sentencia = conexion.prepareStatement(sql)) {

            sentencia.setString(1, estado);
            sentencia.setString(2, id);

            // Devuelve el número de filas actualizadas.
            int filasActualizadas = sentencia.executeUpdate();

            return filasActualizadas > 0;
        }
    }

    // Bloquea una estación para que no pueda iniciar nuevos riegos.
    public static boolean bloquearEstacion(String id) throws SQLException {

        // Solo bloqueamos estaciones que estén disponibles.
        String sql = "UPDATE estaciones SET estado = 'FUERA_SERVICIO' "
                + "WHERE id = ? AND estado = 'DISPONIBLE'";

        try (Connection conexion = conectar();
            PreparedStatement sentencia = conexion.prepareStatement(sql)) {

            sentencia.setString(1, id);

            int filasActualizadas = sentencia.executeUpdate();

            return filasActualizadas > 0;
        }
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

        String sql = "UPDATE estaciones SET estado = 'DISPONIBLE' WHERE id = ? AND estado = 'FUGA'";

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
}