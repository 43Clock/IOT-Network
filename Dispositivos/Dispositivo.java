package Dispositivos;

import org.zeromq.SocketType;
import org.zeromq.ZMQ;
import org.zeromq.ZContext;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class Dispositivo {
    private String id;
    private String password;
    private String tipo;
    private int zona;

    public Dispositivo(String id,String password,String tipo,String zona){
        this.id = id;
        this.password = password;
        this.tipo = tipo;
        this.zona = Integer.parseInt(zona);
    }

    public void start(){
        int porta = 3001 + zona*100;
        try (Socket toColector = new Socket("localhost",porta))
            {
                PrintWriter out = new PrintWriter(toColector.getOutputStream(), true);
                BufferedReader in = new BufferedReader(new InputStreamReader(toColector.getInputStream()));
                out.println("auth:"+this.id+";"+this.password+";"+this.tipo);
                String msg = in.readLine();
                String[] ack = msg.split(":");
                System.out.println(ack[1]);
                if(ack[0].equals("0")) return;
                System.out.println("Atualizar estado:");
                String str;
                boolean flag = true;
                while (flag) {
                    str = System.console().readLine();
                    if (str == null || str.equals("logout")){
                        out.println("logout");
                        flag = false;
                    }else {
                        out.println("evento:"+str);
                    }
                    msg = in.readLine();
                    ack = msg.split(":");
                    System.out.println("Received: " + ack[1]);
                }
            } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
