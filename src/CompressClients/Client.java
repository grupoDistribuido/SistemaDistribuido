package CompressClients;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class Client {

    public static void main(String[] args){
        String IP = args[0];
        int port = Integer.parseInt(args[1]);

        Thread listenerThread = new Thread(new responseListener(IP, port));

        listenerThread.start();











    }

}

class responseListener implements Runnable {

    String IP;
    int port;
    String response;
    public responseListener(String IP, int port) {
        this.IP=IP;
        this.port=port;
    }

    @Override
    public void run() {
        try {
            ServerSocket socket= new ServerSocket(this.port);
            Socket client = socket.accept();


            InputStreamReader input = new InputStreamReader(client.getInputStream());
            BufferedReader reader = new BufferedReader(input);
            while(true){
                response = String.format("Response: %s", reader.readLine());
                System.out.println(response);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}