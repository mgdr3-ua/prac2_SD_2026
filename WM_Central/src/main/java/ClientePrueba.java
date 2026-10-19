import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.io.DataInputStream;

public class ClientePrueba {

    public static void main(String[] args) {

        // Nos conectamos a WM_Central mediante el puerto 5000
        try (Socket socket = new Socket("localhost", 5000)) {

            // Creamos el flujo para enviar datos al servidor
            DataOutputStream salida = new DataOutputStream(socket.getOutputStream());

            // Creamos el flujo para recibir respuestas del servidor
            DataInputStream entrada = new DataInputStream(socket.getInputStream());

            // Probamos el cambio de estado de una estación ya registrada.
            String mensaje = "ESTADO#WS-04#INACTIVA";            
            
            // Enviamos el mensaje a CENTRAL
            salida.writeUTF(mensaje);
            salida.flush();

            System.out.println("Mensaje enviado: " + mensaje);

            // Esperamos la respuesta enviada por CENTRAL
            String respuesta = entrada.readUTF();

            // Mostramos la respuesta recibida
            System.out.println("Respuesta de CENTRAL: " + respuesta);

        } catch (IOException e) {

            // Controlamos posibles errores de comunicación
            System.out.println("Error de conexion: " + e.getMessage());
        }
    }
}
