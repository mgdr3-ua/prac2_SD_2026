import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;

// Se dedica a atender y procesar peticiones de un cliente en concreto

public class hiloServidor extends Thread { // representa hilo de ejecucion 

    private Socket cliente; // cada hilo guarda la conexión del cliente que le corresponde

    public hiloServidor(Socket cliente) {
        this.cliente = cliente;
    }

    public void run() {
        String clienteInfo = cliente.getInetAddress().getHostAddress() + ":" + cliente.getPort();

        try {

            DataInputStream entrada = new DataInputStream(cliente.getInputStream()); // recibir
            DataOutputStream salida = new DataOutputStream(cliente.getOutputStream()); // enviar 

            while (true) {
                String opcion = entrada.readUTF(); //el hilo espera recibir algo

                if (opcion.equals("0")){
                    System.out.println("Cliente " + clienteInfo + " solicita cerrar la conexion");
                    salida.writeUTF("Conexion cerrada correctamente");
                    break;
                }

                String dato = entrada.readUTF();
                String resultado;

                if (opcion.equals("1")) {
                    int numero = Integer.parseInt(dato);
                    resultado = aRomano(numero);

                } else {
                    resultado = String.valueOf(aNumero(dato));
                }

                System.out.println("Recibido: " + dato + " -> Resultado: " + resultado);
                salida.writeUTF(resultado);
            }
            cliente.close();
            System.out.println("Conexion con el cliente " + clienteInfo + " cerrada");

        } catch (Exception e) {
            System.out.println("Cliente desconectado");
        }
    }

    // Numero normal -> romano
    private String aRomano(int numero) {
        int[] valores = {1000,900,500,400,100,90,50,40,10,9,5,4,1};
        String[] romanos = {"M","CM","D","CD","C","XC","L","XL","X","IX","V","IV","I"};

        String resultado = "";

        for (int i = 0; i < valores.length; i++) { // recorremos valores para asignar
            while (numero >= valores[i]) {
                resultado += romanos[i];
                numero -= valores[i];
            }
        }
        return resultado;
    }

    // Romano -> numero normal
    private int aNumero(String romano) {
        int total = 0;

        for (int i = 0; i < romano.length(); i++) {
            int actual = valor(romano.charAt(i));

            if (i + 1 < romano.length() && actual < valor(romano.charAt(i + 1))) {
                total -= actual;

            } else {
                total += actual;
            }
        }
        return total;
    }

    private int valor(char letra) {
        switch (letra) {
            case 'I': return 1;
            case 'V': return 5;
            case 'X': return 10;
            case 'L': return 50;
            case 'C': return 100;
            case 'D': return 500;
            case 'M': return 1000;
        }
        return 0;
    }
}