import java.net.Socket;
import java.io.IOException;

import java.io.DataOutputStream;
import java.io.DataInputStream;

import java.util.Scanner;
import java.util.concurrent.atomic.AtomicBoolean;

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

            

            // Variable compartida que indica si hay una averia
            AtomicBoolean averia = new AtomicBoolean(false);

            // Hilo independiente para detectar las teclas del usuario
            Thread teclado = new Thread(() -> {

                Scanner scanner = new Scanner(System.in);

                System.out.println("Pulsa F + enter para simular una fuga.");
                System.out.println("Pulsa R + enter para resolver la fuga.");

                while (true) {
                    
                    String opcion = scanner.nextLine();

                    if(opcion.equalsIgnoreCase("F")){

                        //Activamos la simulación de fuga
                        averia.set(true);
                        System.out.println("AVERIA SIMULADA: Fuga detectada");
                }else if(opcion.equalsIgnoreCase("R")){

                    //REsolvemos la averia simulada
                    averia.set(false);

                    System.out.println("RECUPERACIÓN: Fuga resuelta correctamente.");
                }
            }
        });

        teclado.setDaemon(true);
        teclado.start();


            while (true) {

                //ESperamos el mensaje del monitor
                String mensaje = entrada.readUTF();

            System.out.println("Mensaje recibido del Monitor: " + mensaje);

            if (mensaje.equals("PING")) {

                if(averia.get()){

                    //Si esxiste una averia -> respondemos KO
                    salida.writeUTF("KO");
                    salida.flush();

                    System.out.println("Respuesta enviada: KO");
                }else{
                    //Si todo funciona -> REspondemos OK     
                    salida.writeUTF("OK");
                    salida.flush();

                    System.out.println("Respuesta enviada: OK");
                }

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