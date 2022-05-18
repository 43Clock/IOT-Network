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

public class Agregador {
    private int zona;
    private List<Integer> vizinhos;
    private Set<String> totalDispositivos;
    private Set<String> dispositivosOnlineZona;
    private Map<String,Set<String>> tiposOnlineZona;
    private Map<String,Set<String>> eventosZona;
    private Map<String,Integer> totalEventosOcorridosZona;


    public Agregador(String zona,List<Integer> vizinhos){
        this.dispositivosOnlineZona = new HashSet<>();
        this.tiposOnlineZona = new HashMap<>();
        this.zona = Integer.parseInt(zona);
        this.vizinhos = new ArrayList<>();
        this.vizinhos.addAll(vizinhos);
    }

    public void start(){
        try (ZContext context = new ZContext();
             ZMQ.Socket fromColector = context.createSocket(SocketType.PULL);
             ZMQ.Socket inform = context.createSocket(SocketType.PUB);
             ZMQ.Socket receive = context.createSocket(SocketType.SUB))
        {
            int portaColetor = 3002 + this.zona*100;
            int portaAgregador = 3003 + this.zona*100;
            //Abre porta para o Coletor se ligar
            fromColector.bind("tcp://*:"+portaColetor);
            //Abre porta para os Agregadores vizinhos se ligarem
            receive.bind("tcp://*:"+portaAgregador);
            //Não sei se é preciso isto
            //receive.subscribe("".getBytes());
            Thread t = new UpdatesHandler(zona, vizinhos, dispositivosOnlineZona, tiposOnlineZona, receive, inform);
            t.start();
            
            for(int v : this.vizinhos)
                //Conecta-se aos agregadores vizinhos
                inform.connect("tcp://localhost:"+(3003+v*100));
            
            //Ciclo para receber as alterações de estados e de login por parte do coletor
            while(true){
                byte[] msg = fromColector.recv();
                String str = new String(msg);
                //Se for um login/logout/registo tem de propagar logo a info
                if (this.processMessage(str)){
                    System.out.println("Send to Super Node to update");
                    inform.send("Login");
                }
                System.out.println("Dispositivos:"+this.dispositivosOnlineZona);
                System.out.println("Tipos:"+this.tiposOnlineZona);
            }
        }
    }

    boolean processMessage(String msg){
        System.out.println("Received:"+msg);
        if(msg.startsWith("login") || msg.startsWith("registo")){
            String[] split = msg.split(":")[1].split(";");
            String id = split[0];
            String tipo = split[1];

            this.dispositivosOnlineZona.add(id);
            this.totalDispositivos.add(id);
            if(!this.tiposOnlineZona.containsKey(split[1])){
                this.tiposOnlineZona.put(tipo,new HashSet<>());
            }
            this.tiposOnlineZona.get(tipo).add(id);

            return true;

//            for(String k: this.tiposOnlineZona.keySet()){
//                if(this.tiposOnlineZona.get(k).contains(split[0])){
//                    this.tiposOnlineZona.get(k).remove(split[0]);
//                    break;
//                }
//            }
        }

        if(msg.startsWith("evento")){
            if(!this.totalEventosOcorridosZona.containsKey(split[1])){
                this.totalEventosOcorridosZona.put(split[1], 1);
            } else {
                this.totalEventosOcorridosZona.replace(split[1],this.totalEventosOcorridosZona.get(split[1])+1);
            }
        }
        if(msg.startsWith("logout")){
            String id = msg.split(":")[1];
            this.dispositivosOnlineZona.remove(id);
            for(String k: this.tiposOnlineZona.keySet()) {
                if (this.tiposOnlineZona.get(k).contains(id)) {
                    this.tiposOnlineZona.get(k).remove(id);
                    notifyDisconnect(id,k);
                    break;
                }
            }
            return true;
        }
        return false;
    }

    //@TODO mudar para o global quando estiver feito
    public int onlineTipo(String tipo){
        return this.tiposOnlineZona.get(tipo).size();
    }

    //@TODO mudar para o global quando estiver feito
    public boolean isOnline(String dispositivo){
        return this.dispositivosOnlineZona.contains(dispositivo);
    }

    //@TODO mudar para o global quando estiver feito
    public int dispositivosOnline(){
        return this.dispositivosOnlineZona.size();
    }

    //@TODO mudar para o global quando estiver feito
    public int totalEventosTipo(String tipo){
        return this.totalEventosOcorridosZona.get(tipo);
    }

    public void notifyDisconnect(String id,String tipo){
        System.out.println("Dispositivo "+id+" do tipo "+ tipo+" desconectou-se");
    }

    //@TODO falta a 2ª notificação

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
