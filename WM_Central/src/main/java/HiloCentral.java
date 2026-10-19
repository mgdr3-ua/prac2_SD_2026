import java.net.Socket;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.DataOutputStream;
import java.sql.SQLException;

public class HiloCentral extends Thread {

    private Socket cliente;

    public HiloCentral(Socket cliente) {
        this.cliente = cliente;
    }

@Override
public void run() {

    try {
        // Mostramos qué cliente está siendo atendido
        System.out.println("Hilo iniciado para el cliente: " + cliente.getInetAddress() + ":" + cliente.getPort());

        // Creamos el flujo para recibir datos del cliente
        DataInputStream entrada = new DataInputStream(cliente.getInputStream());

        // Creamos el flujo para enviar respuestas al cliente
        DataOutputStream salida = new DataOutputStream(cliente.getOutputStream());

        // Esperamos a recibir un mensaje de la estación
        String mensaje = entrada.readUTF();

        // Mostramos el mensaje recibido
        System.out.println("Mensaje recibido: " + mensaje);

        // Separamos el mensaje utilizando # como delimitador
        String[] campos = mensaje.split("#", -1);

        // Comprobamos que el mensaje tenga exactamente 3 campos
        if (campos.length == 3) {
            // Extraemos el tipo de mensaje, el ID y la ubicación
            String tipo = campos[0];
            String idEstacion = campos[1];
            String ubicacion = campos[2];

            // Mostramos los datos obtenidos
            System.out.println("Tipo de mensaje: " + tipo);
            System.out.println("ID de estacion: " + idEstacion);
            System.out.println("Ubicacion: " + ubicacion);

           if (tipo.equals("REGISTRO") && !idEstacion.trim().isEmpty() && !ubicacion.trim().isEmpty()) {

                // Guardamos la estación en SQLite.
                 try {
                    BaseDatos.guardarEstacion(idEstacion, ubicacion);

                    // Solo confirmamos el registro si SQLite funciona.
                    salida.writeUTF("STATUS#OK#Estacion registrada correctamente");
                    salida.flush();

                    } catch (SQLException e) {

                        // Si falla la base de datos, avisamos al cliente.
                        System.err.println("Error al guardar la estacion: " + e.getMessage());

                        salida.writeUTF("STATUS#ERROR#No se pudo guardar la estacion");
                        salida.flush();
                    }
            
            } else if (tipo.equals("ESTADO")) {

                // En este mensaje, el segundo campo es el ID
                // y el tercero indica el nuevo estado de la estación.
                String nuevoEstado = ubicacion;

               // Comprobamos que el ID no esté vacío y que el estado sea válido.
                if (!idEstacion.trim().isEmpty() && (nuevoEstado.equals("DISPONIBLE") || nuevoEstado.equals("REGANDO") || nuevoEstado.equals("FUGA") || nuevoEstado.equals("FUERA_SERVICIO") || nuevoEstado.equals("DESCONECTADA"))) { 

                    try {
                        // Intentamos actualizar la estación en SQLite.
                        boolean actualizada =
                                BaseDatos.actualizarEstado(idEstacion, nuevoEstado);

                        if (actualizada) {
                            salida.writeUTF("STATUS#OK#Estado actualizado correctamente");
                        } else {
                            salida.writeUTF("STATUS#ERROR#Estacion no registrada");
                        }

                    } catch (SQLException e) {

                        // Informamos del error si falla SQLite.
                        System.err.println("Error al actualizar el estado: " + e.getMessage());
                        salida.writeUTF("STATUS#ERROR#No se pudo actualizar el estado");
                    }

                } else {

                    // Rechazamos identificadores vacíos y estados desconocidos.
                    salida.writeUTF("STATUS#ERROR#Datos de estado no validos");
                }

                salida.flush();
            
            } else {

                // El registro contiene datos incorrectos
                System.out.println("ERROR: datos de registro no validos.");

                // Informamos al cliente de que el registro ha fallado
                salida.writeUTF("STATUS#ERROR#Datos de registro no validos");
                salida.flush();
            }   
        
        } else {
             // El mensaje no contiene los tres campos esperados
            System.out.println("ERROR: formato de mensaje incorrecto.");

            // Informamos al cliente del error de formato
            salida.writeUTF("STATUS#ERROR#Formato de mensaje incorrecto");
            salida.flush();
        }

        } catch (IOException e) {
        // Controlamos posibles errores de comunicación
        System.out.println("Error al recibir datos del cliente: " + e.getMessage());

    } finally {
        try {
            // Cerramos la conexión cuando termina el hilo
            cliente.close();
        } catch (IOException e) {
            System.out.println("Error al cerrar la conexion con el cliente.");
        }
    }
}
}