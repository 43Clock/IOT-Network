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
            try (ZContext context = new ZContext();
                 ZMQ.Socket receive = context.createSocket(SocketType.SUB))
            {
                int zona = Integer.parseInt(args[0]);
                receive.connect("tcp://localhost:"+(3004+100*zona));
                receive.subscribe("".getBytes(StandardCharsets.UTF_8));
                while (true){
                    byte[] msg = receive.recv();
                    String str = new String(msg);
                    System.out.println(str);
                }
            }
        }
    }
}
