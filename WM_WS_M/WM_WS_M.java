
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
        int puertoMonitor;
        int puertoCentral;

        try{
            puertoMonitor = Integer.parseInt(args[0]);
            puertoCentral = Integer.parseInt(args[2]);
        }catch(NumberFormatException e){
            System.out.println("ERROR: Los puertos deben ser números.");

            return;
        }


        String ipCentral = args[1];
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


        //Si central rechaza el registro -> NO CONTINUAMOS
        if(!respuesta.startsWith("STATUS#OK#")){
            System.out.println("ERROR: Registro rechazado por Central.");

            return;
        } 
            } catch (IOException e){

            System.out.println("ERROR: No se puede conectar con WM_Central.");
            System.out.println("Motivo: " + e.getMessage());
            
            return;
        }

        //Iniciamos el servidor TCP del monitor para recibir al Engine
        try(ServerSocket servidorMonitor = new ServerSocket(puertoMonitor)){

            System.out.println("Servidor del monitor iniciado.");
            System.out.println("Escuchando en el puerto: " + puertoMonitor);
            System.out.println("Esperando conexion de WM_WS_E ...");

            //Esperamos hasta que el Engine establezca una conexión 
            try(Socket socketEngine = servidorMonitor.accept()){

            System.out.println("WM_WS_E conectado desde: "
                + socketEngine.getInetAddress()
                + ":" + socketEngine.getPort());

            
                //Creamos los flujos para comunicarnos con el Engine
            DataOutputStream salidaEngine = 
                new DataOutputStream(socketEngine.getOutputStream());

            DataInputStream entradaEngine = 
                new DataInputStream(socketEngine.getInputStream());



            //Tiempo máx. de espera de una resp = 3seg
            socketEngine.setSoTimeout(3000);

            //Indica si ya hemos notificado una fuga a CENTRAL
            boolean averiaActiva = false;


             while (true) {
                    
        
            //Enviamos una comprobación de salud al Engine
            salidaEngine.writeUTF("PING");
            salidaEngine.flush();

            System.out.println("Comprobación enviada: PING");

            //Esperamos la respuesta del Engine
            String respuestaEngine = entradaEngine.readUTF();

            System.out.println("Respuesta del Engine: " + respuestaEngine);

            
if (respuestaEngine.equals("OK")) {

    System.out.println("Estado del Engine: SALUDABLE");

    // Si antes habia una averia, notificamos su recuperacion
    if (averiaActiva) {

        String mensajeRecuperacion =
                "AVERIA#" + idEstacion + "#RESUELTA";

        try (Socket socketCentral =
                     new Socket(ipCentral, puertoCentral)) {

            DataOutputStream salidaCentral =
                    new DataOutputStream(socketCentral.getOutputStream());

            DataInputStream entradaCentral =
                    new DataInputStream(socketCentral.getInputStream());

            socketCentral.setSoTimeout(3000);

            salidaCentral.writeUTF(mensajeRecuperacion);
            salidaCentral.flush();

            System.out.println(
                    "Recuperacion enviada: " + mensajeRecuperacion);

            String respuestaCentral = entradaCentral.readUTF();

            System.out.println(
                    "Respuesta de CENTRAL: " + respuestaCentral);

            if (respuestaCentral.startsWith("STATUS#OK#")) {
                averiaActiva = false;
                System.out.println("Estacion recuperada correctamente.");
            } else {
                System.out.println(
                        "CENTRAL no ha confirmado la recuperacion.");
            }

        } catch (IOException e) {
            System.out.println(
                    "ERROR al notificar la recuperacion: "
                    + e.getMessage());
        }
    }

} else if (respuestaEngine.equals("KO")) {

    System.out.println(
            "ALERTA!! Averia detectada en la estacion " + idEstacion);

    // Solo notificamos si aun no hay una averia registrada
    if (!averiaActiva) {

        String mensajeAveria =
                "AVERIA#" + idEstacion + "#FUGA";

        try (Socket socketCentral =
                     new Socket(ipCentral, puertoCentral)) {

            DataOutputStream salidaCentral =
                    new DataOutputStream(socketCentral.getOutputStream());

            DataInputStream entradaCentral =
                    new DataInputStream(socketCentral.getInputStream());

            socketCentral.setSoTimeout(3000);

            salidaCentral.writeUTF(mensajeAveria);
            salidaCentral.flush();

            System.out.println(
                    "Notificacion enviada: " + mensajeAveria);

            String respuestaCentral = entradaCentral.readUTF();

            System.out.println(
                    "Respuesta de CENTRAL: " + respuestaCentral);

            if (respuestaCentral.startsWith("STATUS#OK#")) {
                averiaActiva = true;
                System.out.println("Averia registrada en CENTRAL.");
            } else {
                System.out.println(
                        "CENTRAL no ha confirmado la averia.");
            }

        } catch (IOException e) {
            System.out.println(
                    "ERROR al notificar la averia: "
                    + e.getMessage());
        }
    }

    } else {

        System.out.println("ERROR: Respuesta desconocida del Engine.");
        break;
}
        //Esperamos 1 seg antes de la siguiente comprobación
        Thread.sleep(1000);

            } 
        }


    }catch (InterruptedException e){

        Thread.currentThread().interrupt();
        System.out.println("Monitor interrumpido");

        } catch(IOException e){

            System.out.println("ERROR en el servidor del monitor.");
            System.out.println("Motivo: " + e.getMessage());
        }
    }
}