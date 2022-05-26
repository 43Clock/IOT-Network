package Agregador;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Agregador {
    private int zona;
    private List<Integer> vizinhos;
    private Set<String> totalDispositivos; //local
    private Set<String> dispositivosOnlineZona; //local
    private Set<String> dispositivosOnlineCRDT; //Partilhar, mudar para map para ter tb os tipos e ser um crdt
    private Set<String> dispositivosAtivosCRDT; //Partilhar
    private Map<String,Integer> totalEventosOcorridosCRDT; //Partilhar
    private Map<String,Set<String>> tiposOnline; //local
    private Map<String,Integer> recordTipos; //local
    private AtomicInteger onlineVersion;
    private ZMQ.Socket toClient;

    public Agregador(String zona,List<Integer> vizinhos){
        this.totalDispositivos = new HashSet<>();
        this.dispositivosOnlineZona = new HashSet<>();
        this.dispositivosOnlineCRDT = new HashSet<>();
        this.totalEventosOcorridosCRDT = new HashMap<>();
        this.dispositivosAtivosCRDT = new HashSet<>();
        this.recordTipos = new HashMap<>();
        this.tiposOnline = new HashMap<>();
        this.zona = Integer.parseInt(zona);
        this.vizinhos = new ArrayList<>();
        this.vizinhos.addAll(vizinhos);
        this.onlineVersion = new AtomicInteger(0);
    }

    public void start(){
        try (ZContext context = new ZContext();
             ZMQ.Socket fromColector = context.createSocket(SocketType.PULL);
             ZMQ.Socket inform = context.createSocket(SocketType.PUSH);
             ZMQ.Socket receive = context.createSocket(SocketType.PULL);
             ZMQ.Socket toClient = context.createSocket(SocketType.PUB))
        {
            this.toClient = toClient;
            int portaColetor = 3002 + this.zona*100;
            int portaAgregador = 3003 + this.zona*100;
            int portaCliente = 3004 + this.zona*100;
            //Abre porta para o Coletor se ligar
            fromColector.bind("tcp://*:"+portaColetor);
            //Abre porta para os Agregadores vizinhos se ligarem
            receive.bind("tcp://*:"+portaAgregador);
            toClient.bind("tcp://*:"+portaCliente);
            Thread t = new UpdatesHandler(zona, vizinhos, dispositivosOnlineCRDT, tiposOnline,this.onlineVersion, receive, inform);
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
                    System.out.println("Send to update");
                    this.onlineVersion.addAndGet(1);
                    StringBuilder updateLogin = new StringBuilder("online:").append(this.onlineVersion);
                    updateLogin.append("|");
                    List<String> temp = new ArrayList<>(this.dispositivosOnlineCRDT);
                    for(int i = 0;i<temp.size()-1;i++){
                        updateLogin.append(temp.get(i)).append(",");
                    }
                    if(temp.size() > 0)
                        updateLogin.append(temp.get(temp.size()-1));
                    for (int ignored : this.vizinhos)
                        inform.send(updateLogin.toString());
                }
                System.out.println("Dispositivos:"+this.dispositivosOnlineZona);
                System.out.println("Dispositivos Global:"+this.dispositivosOnlineCRDT);
                System.out.println("Tipos:"+this.tiposOnline);
                System.out.println("Eventos:" + this.totalEventosOcorridosCRDT);
            }
        }
    }

    boolean processMessage(String msg){
        if(msg.startsWith("login") || msg.startsWith("registo")){
            String[] split = msg.split(":")[1].split(";");
            String id = split[0];
            String tipo = split[1];

            //Adiciona aos dispositivos online
            this.dispositivosOnlineZona.add(id);
            this.dispositivosOnlineCRDT.add(id);
            this.totalDispositivos.add(id);

            //Se não existir key com o tipo, cria
            if(!this.tiposOnline.containsKey(split[1])){
                this.tiposOnline.put(tipo,new HashSet<>());
            }
            this.tiposOnline.get(tipo).add(id);

            //Verifica se é record de online
            if(!this.recordTipos.containsKey(tipo)){
                notifyRecordTipo(tipo,1);
                this.recordTipos.put(tipo,1);
            }
            else if(this.recordTipos.get(tipo)<this.tiposOnline.get(tipo).size()){
                int quant = this.tiposOnline.get(tipo).size();
                notifyRecordTipo(tipo,quant);
                this.recordTipos.replace(tipo, quant);
            }

            //Adiciona a ativos
            this.dispositivosAtivosCRDT.add(id);
            return true;
        }

        if(msg.startsWith("evento")){
            String[] split = msg.split(":")[1].split(";");
            String id = split[0];
            String evento = split[1];

            if(!this.totalEventosOcorridosCRDT.containsKey(evento)){
                this.totalEventosOcorridosCRDT.put(evento, 1);
            } else {
                this.totalEventosOcorridosCRDT.replace(split[1],this.totalEventosOcorridosCRDT.get(evento)+1);
            }

            //Adiciona aos ativos caso não esteja
            this.dispositivosAtivosCRDT.add(id);
        }

        if(msg.startsWith("inativo")){
            String id = msg.split(":")[1];
            this.dispositivosAtivosCRDT.remove(id);
            System.out.println(this.dispositivosAtivosCRDT);
        }

        if(msg.startsWith("logout")){
            String id = msg.split(":")[1];
            this.dispositivosOnlineZona.remove(id);
            this.dispositivosOnlineCRDT.remove(id);
            for(String k: this.tiposOnline.keySet()) {
                if (this.tiposOnline.get(k).contains(id)) {
                    this.tiposOnline.get(k).remove(id);
                    if(this.tiposOnline.get(k).size() == 0){
                        notifyNoDevicesTypeOnline(k);
                    }
                    break;
                }
            }

            //Remove dos ativos
            this.dispositivosAtivosCRDT.remove(id);

            return true;
        }
        return false;
    }

    //@TODO mudar para o global quando estiver feito
    public int onlineTipo(String tipo){
        return this.tiposOnline.get(tipo).size();
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
        return this.totalEventosOcorridosCRDT.get(tipo);
    }

    public void notifyNoDevicesTypeOnline(String tipo){
        this.toClient.send("Não existem dispositivos do tipo '"+ tipo+"' online.");
    }

    public void notifyRecordTipo(String tipo, int quant){
        this.toClient.send("Record de dispositivos do tipo '" + tipo + "' atingido ("+quant+" dispostivos)·");
    }

}

class UpdatesHandler extends Thread {
    private int zona;
    private List<Integer> vizinhos;
    private Set<String> dispositivosOnline;
    private Map<String,Set<String>> tipos;
    private AtomicInteger onlineVersion;
    private ZMQ.Socket receive;
    private ZMQ.Socket inform;


    public UpdatesHandler(int zona, List<Integer> vizinhos, Set<String> dispositivosOnline, Map<String, Set<String>> tipos, AtomicInteger onlineVersion, ZMQ.Socket receive, ZMQ.Socket inform) {
        this.zona = zona;
        this.vizinhos = vizinhos;
        this.dispositivosOnline = dispositivosOnline;
        this.tipos = tipos;
        this.onlineVersion = onlineVersion;
        this.receive = receive;
        this.inform = inform;
    }

    public void run(){
        while(true){
            byte[] msg = this.receive.recv();
            String str = new String(msg);
            String[] split = str.split(":");
            switch (split[0]){
                case "online":
                    String[] temp = split[1].split("\\|");
                    int versao = Integer.parseInt(temp[0]);
                    if (versao > this.onlineVersion.get()) {
                        this.onlineVersion.set(versao);
                        if(temp.length >= 2)
                            updateOnline(temp[1]);
                        else updateOnline("");
                        inform.send(str);
                    }
            }
        }
    }

    public void updateOnline(String estado){
        Set<String> estadoSet;
        if(!estado.equals(""))
            estadoSet = new HashSet<>(Arrays.asList(estado.split(",")));
        else
            estadoSet = new HashSet<>();
        System.out.println(estadoSet);
        this.dispositivosOnline.addAll(estadoSet);
        this.dispositivosOnline.removeIf(s -> !estadoSet.contains(s));

    }

}
