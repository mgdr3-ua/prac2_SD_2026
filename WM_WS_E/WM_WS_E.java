import java.net.Socket;
import java.io.IOException;

import java.io.DataOutputStream;
import java.io.DataInputStream;

public class WM_WS_E{

    public static void main(String[] args){

        System.out.println(" --- WATER MANAGER --- ");
        System.out.println("MÓDULO: Engine de estación (WM_WS_E)");

        //Datos de conexión del monitor
        String ipMonitor = "localhost";
        int puertoMonitor = 5001;

        //Intentamos establecer una conexión TCP
        try(Socket socket = new Socket(ipMonitor,puertoMonitor)){

            System.out.println("Conexión establecida con WM_WS_M.");
            System.out.println("Monitor: " + ipMonitor + ":" + puertoMonitor);

            //Creamos los flujos de comunicación
            DataInputStream entrada = 
                new DataInputStream(socket.getInputStream());

            DataOutputStream salida =
                new DataOutputStream(socket.getOutputStream());

            

            while (true) {

                //ESperamos el mensaje del monitor
                String mensaje = entrada.readUTF();

            System.out.println("Mensaje recibido del Monitor: " + mensaje);

            if (mensaje.equals("PING")) {

                //Respondemos indicando que el Engine funciona
                salida.writeUTF("OK");
                salida.flush();

                System.out.println("Respuesta enviada: OK!");
            }else{

                System.out.println("Mensaje desconocido: " + mensaje);
            }
        
        }
    } catch (IOException e){

        System.out.println("ERROR: comunicación con WM_WS_M.");
        System.out.println("Motivo: " + e.getMessage());
        
        }
    }
}