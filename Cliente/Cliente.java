package Cliente;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.nio.charset.StandardCharsets;

public class Cliente {

    public static void main(String[] args) {
        if(args.length < 1){
            System.out.println("Argumentos insuficientes");
        } else {
            try {
                ListenToSubs thread = new ListenToSubs(Integer.parseInt(args[0]));
                thread.start();
            } catch (Exception e){
                System.out.println("Zona inválida, tem de ser um numero!");
            }

        }
    }
}

class ListenToSubs extends Thread {
    private int zona;

    public ListenToSubs(int zona) {
        this.zona = zona;
    }

    public void run(){
        try (ZContext context = new ZContext();
             ZMQ.Socket receive = context.createSocket(SocketType.SUB))
        {

            receive.connect("tcp://localhost:"+(3004+100*zona));
            receive.subscribe("60".getBytes(StandardCharsets.UTF_8));
            while (true){
                byte[] msg = receive.recv();
                String str = new String(msg);
                System.out.println(str);
            }
        }
    }

}