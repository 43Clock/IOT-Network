package Dispositivos;

import org.zeromq.SocketType;
import org.zeromq.ZMQ;
import org.zeromq.ZContext;

public class Dispositivo {
    private String id;
    private String password;
    private String tipo;

    public Dispositivo(String id,String password){
        this.id = id;
        this.password = password;
    }

    public void start(){
        try (ZContext context = new ZContext();
             ZMQ.Socket toColector = context.createSocket(SocketType.REQ))
            {
                toColector.connect("tcp://localhost:3001");
                toColector.send("auth:"+this.id+";"+this.password);
                byte[] msg = toColector.recv();
                String ack = new String(msg);
                System.out.println(ack);
                System.out.println("Atualizar estado:\n");
                String str;
                while (true) {
                    str = System.console().readLine();
                    if (str == null) break;
                    toColector.send("tipo:"+this.id+";"+str);
                    msg = toColector.recv();
                    ack = new String(msg);
                    System.out.println("Received: " + ack);
                }
            }
    }
}
