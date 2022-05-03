import org.zeromq.SocketType;
import org.zeromq.ZMQ;
import org.zeromq.ZContext;

class Agregador {
    public static void main(String[] args) {
        try (ZContext context = new ZContext();
             ZMQ.Socket sink = context.createSocket(SocketType.PULL))
        {
            sink.bind("tcp://*:3002");
            while(true){
                byte[] msg = sink.recv();
                String str = new String(msg);
                System.out.println("Received " + str);
            }
        }
    }
}