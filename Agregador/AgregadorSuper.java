package Agregador;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class AgregadorSuper extends Agregador {

    AgregadorSuper(Set<String> dispositiovosOnline,Map<String,Set<String>> tipos,String zona){
        super(dispositiovosOnline,tipos,zona,zona);
    }

    public void run(){
        SuperThread thread = new SuperThread(this.dispositivosOnline,this.tipos,this.zona);
        thread.start();
    }

    @Override
    public void start(){
        try (ZContext context = new ZContext();
             ZMQ.Socket fromColector = context.createSocket(SocketType.PULL);
             ZMQ.Socket agregador = context.createSocket(SocketType.REP))
        {
            int portaColetor = 3002 + this.zona*100;
            int portaAgregador = 3003 + this.zona*100;
            fromColector.bind("tcp://*:"+portaColetor);
            agregador.bind("tcp://*:"+portaAgregador);
            while(true){
                byte[] msg = fromColector.recv();
                String str = new String(msg);
                if(this.processMessage(str)){
                    System.out.println("Propagar a alteração");
                }
                System.out.println("Dispositivos:"+this.dispositivosOnline);
                System.out.println("Tipos:"+this.tipos);
            }
        }
    }
}


class SuperThread extends Thread{
    private int zona;
    private Set<String> dispositivosOnline;
    private Map<String,Set<String>> tipos;

    SuperThread(Set<String> dispositiovosOnline,Map<String,Set<String>> tipos, int zona){
        this.dispositivosOnline = dispositiovosOnline;
        this.tipos = tipos;
        this.zona = zona;
    }

    public void run(){
        while(true){
            System.out.println(dispositivosOnline);
            System.out.println(tipos);
            try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
        }
    }
}