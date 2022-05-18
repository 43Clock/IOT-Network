package Dispositivos;

import org.zeromq.SocketType;
import org.zeromq.ZMQ;
import org.zeromq.ZContext;

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
        try (ZContext context = new ZContext();
             ZMQ.Socket toColector = context.createSocket(SocketType.REQ))
            {
                int porta = 3001 + zona*100;
                toColector.connect("tcp://localhost:"+porta);
                toColector.send("auth:"+this.id+";"+this.password+";"+this.tipo);
                byte[] msg = toColector.recv();
                String[] ack = new String(msg).split(":");
                System.out.println(ack[1]);
                if(ack[0].equals("0")) return;
                System.out.println("Atualizar estado:");
                String str;
                boolean flag = true;
                while (flag) {
                    str = System.console().readLine();
                    if (str == null || str.equals("logout")){
                        toColector.send("logout:"+this.id);
                        flag = false;
                    }else {
                        toColector.send("tipo:"+this.id+";"+str);
                    }
                    msg = toColector.recv();
                    ack = new String(msg).split(":");
                    System.out.println("Received: " + ack[1]);
                }
            }
    }
}
