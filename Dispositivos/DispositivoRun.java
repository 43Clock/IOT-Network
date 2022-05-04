package Dispositivos;

import org.zeromq.SocketType;
import org.zeromq.ZMQ;
import org.zeromq.ZContext;

public class DispositivoRun {
    public static void main(String[] args) {
        if(args.length < 3) {
            System.out.println("Argumentos insuficientes!");
        } else {
            Dispositivo d = new Dispositivo(args[0],args[1],args[2]);
            try (ZContext context = new ZContext();
                 ZMQ.Socket toColector = context.createSocket(SocketType.REQ))
    
            {
                toColector.connect("tcp://localhost:3001");
                toColector.send("auth:"+args[0]+";"+args[1]+";"+args[2]);
                byte[] msg = toColector.recv();
                String str = new String(msg);
                System.out.println("Received " + str);
                while (true) {
                    str = System.console().readLine();
                    if (str == null) break;
                    toColector.send(str);
                }
            }
        }
    }    
}
