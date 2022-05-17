package Agregador;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class Agregador {
    private int zona;
    private List<Integer> vizinhos;
    private Set<String> dispositivosOnline;
    private Map<String,Set<String>> tipos;


    public Agregador(String zona,List<Integer> vizinhos){
        this.dispositivosOnline = new HashSet<String>();
        this.tipos = new HashMap<String,Set<String>>();
        this.zona = Integer.parseInt(zona);
        this.vizinhos = new ArrayList<>();
        for (int v: vizinhos)
            this.vizinhos.add(v);
    }

    public void start(){
        try (ZContext context = new ZContext();
             ZMQ.Socket fromColector = context.createSocket(SocketType.PULL);
             ZMQ.Socket inform = context.createSocket(SocketType.PUB);
             ZMQ.Socket receive = context.createSocket(SocketType.SUB))
        {
            int portaColetor = 3002 + this.zona*100;
            int portaAgregador = 3003 + this.zona*100;
            fromColector.bind("tcp://*:"+portaColetor);
            receive.bind("tcp://*:"+portaAgregador);
            receive.subscribe("".getBytes());
            Thread t = new UpdatesHandler(zona, vizinhos, dispositivosOnline, tipos, receive, inform);
            t.start();
            
            for(int v : this.vizinhos)
                inform.connect("tcp://localhost:"+(3003+v*100));
            while(true){
                byte[] msg = fromColector.recv();
                String str = new String(msg);
                if (this.processMessage(str)){
                    System.out.println("Send to Super Node to update");
                    inform.send("Login");
                }
                System.out.println("Dispositivos:"+this.dispositivosOnline);
                System.out.println("Tipos:"+this.tipos);
            }
        }
    }

    boolean processMessage(String msg){
        System.out.println("Received:"+msg);
        if(msg.startsWith("login") || msg.startsWith("registo")){
            this.dispositivosOnline.add(msg.split(":")[1]);
            return true;
        }

        if(msg.startsWith("tipo")){
            String[] split = msg.split(":")[1].split(";");
            for(String k: this.tipos.keySet()){
                if(this.tipos.get(k).contains(split[0])){
                    this.tipos.get(k).remove(split[0]);
                    break;
                }
            }
            if(!this.tipos.containsKey(split[1])){
                this.tipos.put(split[1],new HashSet<>());
            }
            this.tipos.get(split[1]).add(split[0]);
        }
        if(msg.startsWith("logout")){
            String id = msg.split(":")[1];
            this.dispositivosOnline.remove(id);
            for(String k: this.tipos.keySet()) {
                if (this.tipos.get(k).contains(id)) {
                    this.tipos.get(k).remove(id);
                    break;
                }
            }
            return true;
        }
        return false;
        //Filtra vazios
        // this.tipos = this.tipos.entrySet().stream().filter(entry->entry.getValue().size() != 0)
        //         .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}

class UpdatesHandler extends Thread {
    private int zona;
    private List<Integer> vizinhos;
    private Set<String> dispositivosOnline;
    private Map<String,Set<String>> tipos;
    private ZMQ.Socket receive;
    private ZMQ.Socket inform;
    
    UpdatesHandler(int zona, List<Integer> vizinhos,Set<String> dispositivosOnline
                  ,Map<String,Set<String>> tipos,ZMQ.Socket receive,ZMQ.Socket inform)
    {
        this.zona = zona;
        this.vizinhos = vizinhos;
        this.dispositivosOnline = dispositivosOnline;
        this.tipos = tipos;
        this.receive = receive;
        this.inform = inform;
    }
    
    public void run(){
        while(true){
            byte[] msg = this.receive.recv();
            String str = new String(msg);
            System.out.println(str);
        }
    }

}
