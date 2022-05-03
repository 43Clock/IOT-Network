import org.zeromq.SocketType;
import org.zeromq.ZMQ;
import org.zeromq.ZContext;

public class Dispositivo {
    public static void main(String[] args) {
        try (ZContext context = new ZContext();
             ZMQ.Socket toColector = context.createSocket(SocketType.PUSH))

        {
            toColector.connect("tcp://localhost:3001");
            while (true) {
                String str = System.console().readLine();
                if (str == null) break;
                toColector.send(str);
            }
        }
    }    
}
