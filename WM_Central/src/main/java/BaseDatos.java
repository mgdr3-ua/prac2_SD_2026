import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

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
                ubicacion TEXT NOT NULL
            )
            """;

        // Abrimos la conexión y ejecutamos la sentencia SQL.
        // Los recursos se cierran automáticamente al terminar.
        try (Connection conexion = conectar();
             Statement sentencia = conexion.createStatement()) {

            sentencia.executeUpdate(sql);
            System.out.println("Tabla de estaciones preparada correctamente.");
        }
    }

    
    // Guarda una estación en la base de datos SQLite.
    // Si la estación ya existe, actualiza su ubicación.
    public static void guardarEstacion(String id, String ubicacion) throws SQLException {

        // Consulta SQL para insertar o actualizar una estación.
        String sql = """
            INSERT INTO estaciones (id, ubicacion)
            VALUES (?, ?)
            ON CONFLICT(id) DO UPDATE SET ubicacion = excluded.ubicacion
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
}