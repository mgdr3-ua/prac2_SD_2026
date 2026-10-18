
import java.net.Socket;
import java.io.IOException;

import java.io.DataInputStream;
import java.io.DataOutputStream;

import java.net.ServerSocket;

public class WM_WS_M{

    public static void main(String[] args){ 

        System.out.println("--- WATER MANAGEMENT --- ");
        System.out.println("Módulo: Monitor de estación (WM_WS_M)");

        //Comprobamos los parámetros de entrada:
        if(args.length != 4){
            System.out.println("Uso: java WM_WS_M <puerto_monitor> <ip_central> <puerto_central> <id_estacion>");
            return;
        }

        //Guardamos los parámetros:
        int puertoMonitor = Integer.parseInt(args[0]);
        String ipCentral = args[1];
        int puertoCentral = Integer.parseInt(args[2]);
        String idEstacion = args[3];

        // Mostramos la confiuración:
        System.out.println("ID Eestación: " + idEstacion);
        System.out.println("Puerto monitor: " + puertoMonitor);
        System.out.println("IP central: " + ipCentral);
        System.out.println("Puerto central: " + puertoCentral);    

        System.out.println("Monitor iniciado correctamente!! ");

        //Intentamos conectar WM_Central
        try(Socket socket = new Socket(ipCentral, puertoCentral)){
            
            System.out.println("Conexion establecida con WM_Central.");

            //Creamos los flujos de comunicación:
            DataOutputStream salida = 
                new DataOutputStream(socket.getOutputStream());

            DataInputStream entrada = 
                new DataInputStream(socket.getInputStream());

        
        //Construimos el mesanje de registro:
        String ubicacion = "River Park";
        String mensaje = "REGISTRO#" + idEstacion + "#" + ubicacion;

        //Enviamos el registro a CENTRAL
        salida.writeUTF(mensaje);
        salida.flush();

        System.out.println("Mensaje enviado: " + mensaje);

        //REspuesta de la central:
        String respuesta = entrada.readUTF();

        System.out.println("Respuesta de la Central: " + respuesta);

        } catch (IOException e){

            System.out.println("ERROR: No se puede conectar con WM_Central.");
            System.out.println("Motivo: " + e.getMessage());
        }

        //Iniciamos el servidor TCP del monitor para recibir al Engine
        try(ServerSocket servidorMonitor = new ServerSocket(puertoMonitor)){

            System.out.println("Servidor del monitor iniciado.");
            System.out.println("Escuchando en el puerto: " + puertoMonitor);
            System.out.println("Esperando conexion de WM_WS_M ...");

            //Esperamos hasta que el Engine establezca una conexión 
            Socket socketEngine = servidorMonitor.accept();

            System.out.println("WM_WS_E conectado desde: "
                + socketEngine.getInetAddress()
                + ":" + socketEngine.getPort()
            );

            //Por ahora cerraremos la conexion de prueba
            socketEngine.close();
        } catch(IOException e){

            System.out.println("ERROR en el servidor del monitor.");
            System.out.println("Motivo: " + e.getMessage());
        }
    }
}