package Agregador;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class Agregador {
    private int zona;
    private List<Integer> vizinhos;
    private Map<Integer,Map<String,Set<String>>> dispositivosOnlineCRDT; //Partilhar, zona->tipo->lista de ids
    private Map<Integer,Set<String>> dispositivosAtivosCRDT; //Partilhar
    private Map<Integer,Map<String,Integer>> totalEventosOcorridosCRDT; //Partilhar, zona->tipo->quantidade
    private Map<String,Integer> recordTipos; //local
    private AtomicInteger onlineVersion;
    private ZMQ.Socket toClient;

    public Agregador(String zona,List<Integer> vizinhos){
        this.zona = Integer.parseInt(zona);
        this.dispositivosOnlineCRDT = new HashMap<>();
        //Inicializa o mapa para a zona em questao
        this.dispositivosOnlineCRDT.put(this.zona,new HashMap<>());
        this.dispositivosAtivosCRDT = new HashMap<>();
        //Inicializa o mapa para a zona em questao
        this.dispositivosAtivosCRDT.put(this.zona,new HashSet<>());
        this.totalEventosOcorridosCRDT = new HashMap<>();
        //Inicializa o mapa para a zona em questao
        this.totalEventosOcorridosCRDT.put(this.zona,new HashMap<>());
        this.recordTipos = new HashMap<>();
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

            //Thread que trata de receber mensagens dos outros agregadores
            Thread t = new UpdatesHandler(zona, vizinhos, dispositivosOnlineCRDT,dispositivosAtivosCRDT,totalEventosOcorridosCRDT, receive, inform);
            t.start();

            for(int v : this.vizinhos)
                //Conecta-se aos agregadores vizinhos
                inform.connect("tcp://localhost:"+(3003+v*100));

            //Scheduler para mandar updates os outros agregadores
            ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
            executor.scheduleAtFixedRate(()->{
                for(int ignore: this.vizinhos)
                    inform.send(this.serializeAtivos());
                    inform.send(this.serializeEventos());
            },15,15, TimeUnit.SECONDS);
            
            //Ciclo para receber as alterações de estados e de login por parte do coletor
            while(true){
                byte[] msg = fromColector.recv();
                String str = new String(msg);
                //Se for um login/logout/registo tem de propagar logo a info
                if (this.processMessage(str)){
                    System.out.println("Send to update");
                    String serialized = this.serializeOnline();
                    for(int ignore:this.vizinhos)
                        inform.send(serialized);
                }
                System.out.println("Dispositivos:"+this.dispositivosOnlineCRDT.get(this.zona));
                System.out.println("Dispositivos Global:"+this.dispositivosOnlineCRDT);
                System.out.println("Eventos:" + this.totalEventosOcorridosCRDT.get(this.zona));
            }
        }
    }

    private String serializeOnline(){
        //online-1:tipo1->a,b;tipo2->c,d
        StringJoiner sj = new StringJoiner(";","online-"+this.zona+":","");
        for(Map.Entry<String,Set<String>> entry:this.dispositivosOnlineCRDT.get(this.zona).entrySet()){
            StringJoiner temp = new StringJoiner(",",entry.getKey()+"->","");
            for(String s:entry.getValue())
                temp.add(s);
            sj.add(temp.toString());
        }
        return sj.toString();
    }

    private String serializeAtivos(){
        //ativos-1:a,b,c
        StringJoiner sj = new StringJoiner(",", "ativos-" + this.zona + ":", "");
        for(String disp : this.dispositivosAtivosCRDT.get(this.zona))
            sj.add(disp);
        return sj.toString();
    }

    private String serializeEventos(){
        //eventos-1:evento1-x;evento2-y
        StringJoiner sj = new StringJoiner(";","eventos-"+this.zona+":","");
        for (Map.Entry<String, Integer> entry : this.totalEventosOcorridosCRDT.get(this.zona).entrySet()) {
            sj.add(entry.getKey()+"-"+entry.getValue());
        }
        return sj.toString();
    }

    private boolean processMessage(String msg){
        if(msg.startsWith("login") || msg.startsWith("registo")){
            String[] split = msg.split(":")[1].split(";");
            String id = split[0];
            String tipo = split[1];
            Map<String, Set<String>> mapZone = this.dispositivosOnlineCRDT.get(this.zona);

            //Se não existir key com o tipo, cria
            if(!mapZone.containsKey(split[1])){
                mapZone.put(tipo,new HashSet<>());
            }
            mapZone.get(tipo).add(id);

            //Verifica se é record de online
            if(!this.recordTipos.containsKey(tipo)){
                notifyRecordTipo(tipo,1);
                this.recordTipos.put(tipo,1);
            }
            else if(this.recordTipos.get(tipo)<mapZone.get(tipo).size()){
                int quant = mapZone.get(tipo).size();
                notifyRecordTipo(tipo,quant);
                this.recordTipos.replace(tipo, quant);
            }

            //Verifica percentagem
            int percentagem = getTotalDispositivosZona() * 100 / getTotalDispositivos();
            while(percentagem %10 != 0)
                percentagem--;
            notificaAcimaDe(percentagem);
            notificaAbaixoDe(percentagem+10);

            //Adiciona a ativos
            this.dispositivosAtivosCRDT.get(this.zona).add(id);
            return true;
        }

        if(msg.startsWith("evento")){
            String[] split = msg.split(":")[1].split(";");
            String id = split[0];
            String evento = split[1];
            Map<String, Integer> mapZona = this.totalEventosOcorridosCRDT.get(this.zona);

            if(!mapZona.containsKey(evento)){
                mapZona.put(evento, 1);
            } else {
                mapZona.replace(split[1],mapZona.get(evento)+1);
            }

            //Adiciona aos ativos caso não esteja
            this.dispositivosAtivosCRDT.get(this.zona).add(id);
        }

        if(msg.startsWith("inativo")){
            String id = msg.split(":")[1];
            this.dispositivosAtivosCRDT.get(this.zona).remove(id);
            System.out.println("Ativos: "+this.dispositivosAtivosCRDT);
        }

        if(msg.startsWith("logout")){
            String id = msg.split(":")[1];
            Map<String, Set<String>> mapZona = this.dispositivosOnlineCRDT.get(this.zona);
            //Remove dos online da zona
            for(String tipo: mapZona.keySet())
                if (mapZona.get(tipo).contains(id)) {
                    mapZona.get(tipo).remove(id);
                    if(mapZona.get(tipo).size() == 0)
                        notifyNoDevicesTypeOnline(tipo);
                    break;
                }

            //Verifica percentagem
            int percentagem = getTotalDispositivosZona() * 100 / getTotalDispositivos();
            while(percentagem %10 != 0)
                percentagem--;
            notificaAcimaDe(percentagem);
            notificaAbaixoDe(percentagem+10);

            //Remove dos ativos
            this.dispositivosAtivosCRDT.get(this.zona).remove(id);
            return true;
        }

        return false;
    }

    public int getTotalDispositivos(){
        int contador = 0;
        for(Map<String, Set<String>> zona:this.dispositivosOnlineCRDT.values())
            contador += zona.values().stream().map(Set::size).reduce(0, Integer::sum);
        return contador;
    }

    public int getTotalDispositivosZona(){
        return this.dispositivosOnlineCRDT.get(this.zona).values().stream().map(Set::size).reduce(0, Integer::sum);
    }

    public int onlineTipo(String tipo){
        int contador = 0;
        for(int zona: this.dispositivosOnlineCRDT.keySet())
            contador += this.dispositivosOnlineCRDT.get(zona).get(tipo).size();
        return contador;
    }

    public boolean isOnline(String dispositivo){
        for (int zona : this.dispositivosOnlineCRDT.keySet()) {
            for (String tipo : this.dispositivosOnlineCRDT.get(zona).keySet()) {
                if(this.dispositivosOnlineCRDT.get(zona).get(tipo).contains(dispositivo))
                    return true;
            }
        }
        return false;
    }

    public int dispositivosAtivos(){
        int contador = 0;
        for(int key: this.dispositivosAtivosCRDT.keySet())
            contador += this.dispositivosAtivosCRDT.get(key).size();
        return contador;
    }

    public int totalEventosTipo(String tipo) {
        int total = 0;
        for (int key : this.totalEventosOcorridosCRDT.keySet())
            total += this.totalEventosOcorridosCRDT.get(key).get(tipo);
        return total;
    }

    public void notifyNoDevicesTypeOnline(String tipo){
        this.toClient.send("online-Não existem dispositivos do tipo '"+ tipo+"' online.");
    }

    public void notifyRecordTipo(String tipo, int quant){
        this.toClient.send("record-Record de dispositivos do tipo '" + tipo + "' atingido ("+quant+" dispostivos)·");
    }

    public void notificaAcimaDe(int percentagem){
        while(percentagem >0){
            this.toClient.send(percentagem+"-Percentagem de dispositivos passou os "+percentagem+"%.");
            percentagem-=10;
        }
    }

    public void notificaAbaixoDe(int percentagem){
        while(percentagem < 100){
            this.toClient.send(percentagem+"-Percentagem de dispositivos desceu dos "+percentagem+"%.");
            percentagem+=10;
        }
    }

}

class UpdatesHandler extends Thread {
    private int zona;
    private List<Integer> vizinhos;
    private Map<Integer,Map<String,Set<String>>> dispositivosOnlineCRDT;
    private Map<Integer,Set<String>> dispositivosAtivosCRDT;
    private Map<Integer,Map<String,Integer>> totalEventosOcorridosCRDT;
    private ZMQ.Socket receive;
    private ZMQ.Socket inform;

    public UpdatesHandler(int zona, List<Integer> vizinhos, Map<Integer, Map<String, Set<String>>> dispositivosOnlineCRDT, Map<Integer, Set<String>> dispositivosAtivosCRDT, Map<Integer, Map<String, Integer>> totalEventosOcorridosCRDT, ZMQ.Socket receive, ZMQ.Socket inform) {
        this.zona = zona;
        this.vizinhos = vizinhos;
        this.dispositivosOnlineCRDT = dispositivosOnlineCRDT;
        this.dispositivosAtivosCRDT = dispositivosAtivosCRDT;
        this.totalEventosOcorridosCRDT = totalEventosOcorridosCRDT;
        this.receive = receive;
        this.inform = inform;
    }

    public void run(){
        while(true){
            //online-1:tipo1->a,b;tipo2->c,d
            //ativos-1:a,b,c
            //eventos-1:evento1-x;evento2-y
            byte[] msg = this.receive.recv();
            String str = new String(msg);
            System.out.println("Received: "+str);
            String[] split = str.split(":");
            String[] tipoZona = split[0].split("-");
            switch (tipoZona[0]){
                case "online":
                    if(split.length>1){
                        mergeDispositivosOnlineCRDT(Integer.parseInt(tipoZona[1]), deserializeOnline(split[1]));
                        System.out.println("CRDT-Online: " + this.dispositivosOnlineCRDT);
                    }
                    break;
                case "ativos":
                    if(split.length>1){
                        mergeDispositivosAtivosCRDT(Integer.parseInt(tipoZona[1]), deserializeAtivos(split[1]));
                    }else{
                        mergeDispositivosAtivosCRDT(Integer.parseInt(tipoZona[1]), new HashSet<>());
                    }
                    System.out.println("CRDT-Ativos: " + this.dispositivosAtivosCRDT);
                    break;
                case "eventos":
                    if(split.length>1){
                        mergeEventosCRDT(Integer.parseInt(tipoZona[1]), deserializeEventos(split[1]));
                    }
                    System.out.println("CRDT-Eventos: " + this.totalEventosOcorridosCRDT);
                default:
                    break;
            }
            //@TODO falta fazer a cena das percentagens para quando recebe info de login de outros agregadores
            //@TODO falta reenviar para os vizinhos e evitar que repita para o source da mensagem
        }
    }

    public Map<String,Set<String>> deserializeOnline(String input){
        //tipo1->a,b;tipo2->c,d
        Map<String, Set<String>> result = new HashMap<>();
        String[] tipos = input.split(";");
        for(String tipo:tipos){
            String[] split = tipo.split("->");
            result.put(split[0], new HashSet<>());
            if(split.length > 1){
                String[] dispositivos = split[1].split(",");
                for(String dispositivo:dispositivos)
                    result.get(split[0]).add(dispositivo);
            }
        }
        return result;
    }

    public Set<String> deserializeAtivos(String input){
        String[] disp = input.split(",");
        return new HashSet<>(Arrays.asList(disp));
    }

    public Map<String, Integer> deserializeEventos(String input) {
        //evento1-x;evento2-y
        Map<String, Integer> res = new HashMap<>();
        String[] eventos = input.split(";");
        for(String evento: eventos){
            String[] split = evento.split("-");
            res.put(split[0],Integer.parseInt(split[1]));
        }
        return res;
    }

    public void mergeDispositivosOnlineCRDT(int zona, Map<String, Set<String>> toMerge) {
        if(!this.dispositivosOnlineCRDT.containsKey(zona)){
            this.dispositivosOnlineCRDT.put(zona, new HashMap<>());
            Map<String, Set<String>> zonaValues = this.dispositivosOnlineCRDT.get(zona);
            for(Map.Entry<String,Set<String>> entry: toMerge.entrySet()){
                zonaValues.put(entry.getKey(), new HashSet<>());
                Set<String> setTipo = zonaValues.get(entry.getKey());
                setTipo.addAll(entry.getValue());
            }
        }
        else {
            Map<String, Set<String>> zonaValues = this.dispositivosOnlineCRDT.get(zona);
            //Primeiro esvaziar tudo o que tem
            for (Map.Entry<String, Set<String>> entry : this.dispositivosOnlineCRDT.get(zona).entrySet())
                entry.getValue().clear();
            //Depois preencher com nova info
            for (Map.Entry<String, Set<String>> entry : toMerge.entrySet()) {
                zonaValues.putIfAbsent(entry.getKey(), new HashSet<>());
                Set<String> setTipo = zonaValues.get(entry.getKey());
                setTipo.addAll(entry.getValue());

            }
        }
    }

    public void mergeDispositivosAtivosCRDT(int zona, Set<String> toMerge){
        if(!this.dispositivosAtivosCRDT.containsKey(zona)){
            this.dispositivosAtivosCRDT.put(zona,new HashSet<>());
        }
        else{
            this.dispositivosAtivosCRDT.get(zona).clear();
        }
        this.dispositivosAtivosCRDT.get(zona).addAll(toMerge);
    }

    public void mergeEventosCRDT(int zona, Map<String,Integer> toMerge){
        if(!this.totalEventosOcorridosCRDT.containsKey(zona))
            this.totalEventosOcorridosCRDT.put(zona, new HashMap<>());
        Map<String,Integer> zonaMapa = this.totalEventosOcorridosCRDT.get(zona);
        for(Map.Entry<String,Integer> entry: toMerge.entrySet()){
            if (zonaMapa.containsKey(entry.getKey())) {
                zonaMapa.replace(entry.getKey(), entry.getValue());
            }
            else{
                zonaMapa.put(entry.getKey(),entry.getValue());
            }
        }
    }
}
