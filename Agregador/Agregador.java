package Agregador;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class Agregador {
    private Set<String> dispositivosOnline;
    private Map<String,Set<String>> tipos;

    public Agregador(){
        this.dispositivosOnline = new HashSet<>();
        this.tipos = new HashMap<>();
    }

    public void start(){
        try (ZContext context = new ZContext();
             ZMQ.Socket fromColector = context.createSocket(SocketType.PULL))
        {
            fromColector.bind("tcp://*:3002");
            while(true){
                byte[] msg = fromColector.recv();
                String str = new String(msg);
                this.processMessage(str);
                System.out.println("Dispositivos:"+this.dispositivosOnline);
                System.out.println("Tipos:"+this.tipos);
            }
        }
    }

    void processMessage(String msg){
        System.out.println("Received:"+msg);
        if(msg.startsWith("login") || msg.startsWith("registo")){
            this.dispositivosOnline.add(msg.split(":")[1]);
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
        }
        //Filtra vazios
        this.tipos = this.tipos.entrySet().stream().filter(entry->entry.getValue().size() != 0)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}
