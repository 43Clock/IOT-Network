package Agregador;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Agregador {
    private int zona;
    private List<Integer> vizinhos;
    private Map<Integer,Map<String,Set<String>>> dispositivosOnlineCRDT; //Partilhar, zona->tipo->lista de ids
    private Map<Integer,Set<String>> dispositivosAtivosCRDT; //Partilhar
    private Map<Integer,Map<String,Integer>> totalEventosOcorridosCRDT; //Partilhar, zona->tipo->quantidade
    private Map<String,Integer> recordTipos; //local
    private Map<Integer,Map<Integer,Integer>> versions; //Zona->CRDT->Versao
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
        this.versions = new HashMap<>();
        this.versions.put(this.zona, new HashMap<>());
        for(int i = 1;i<=3;i++)
            this.versions.get(this.zona).put(i,0);
    }

    public void start(){
        try (ZContext context = new ZContext();
             ZMQ.Socket fromColector = context.createSocket(SocketType.PULL);
             ZMQ.Socket inform = context.createSocket(SocketType.PUSH);
             ZMQ.Socket receive = context.createSocket(SocketType.PULL);
             ZMQ.Socket toClient = context.createSocket(SocketType.PUB);
             ZMQ.Socket fromClient = context.createSocket(SocketType.REP))
        {
            this.toClient = toClient;
            int portaColetor = 3002 + this.zona*100;
            int portaAgregador = 3003 + this.zona*100;
            int portaCliente = 3004 + this.zona*100;
            int portaClienteIn = 3005 + this.zona*100;
            //Abre porta para o Coletor se ligar
            fromColector.bind("tcp://*:"+portaColetor);
            //Abre porta para os Agregadores vizinhos se ligarem
            receive.bind("tcp://*:"+portaAgregador);
            toClient.bind("tcp://*:"+portaCliente);
            fromClient.bind("tcp://*:"+portaClienteIn);

            //Thread que trata de receber mensagens dos outros agregadores
            Thread t = new UpdatesHandler(zona, vizinhos, dispositivosOnlineCRDT,dispositivosAtivosCRDT,totalEventosOcorridosCRDT,versions, receive, inform, toClient);
            t.start();

            //Thread que trata dos pedidos do cliente
            Thread tc = new ClienteHandler(zona,dispositivosOnlineCRDT,dispositivosAtivosCRDT,totalEventosOcorridosCRDT,fromClient);
            tc.start();

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
        //online-1.v:tipo1->a,b;tipo2->c,d
        this.versions.get(this.zona).replace(1,this.versions.get(this.zona).get(1)+1);
        int versao = this.versions.get(this.zona).get(1);
        StringJoiner sj = new StringJoiner(";","online-"+this.zona+"."+versao+":","");
        for(Map.Entry<String,Set<String>> entry:this.dispositivosOnlineCRDT.get(this.zona).entrySet()){
            StringJoiner temp = new StringJoiner(",",entry.getKey()+"->","");
            for(String s:entry.getValue())
                temp.add(s);
            sj.add(temp.toString());
        }
        return sj.toString();
    }

    private String serializeAtivos(){
        //ativos-1.v:a,b,c
        this.versions.get(this.zona).replace(2,this.versions.get(this.zona).get(2)+1);
        int versao = this.versions.get(this.zona).get(2);
        StringJoiner sj = new StringJoiner(",", "ativos-" + this.zona +"."+versao + ":", "");
        for(String disp : this.dispositivosAtivosCRDT.get(this.zona))
            sj.add(disp);
        return sj.toString();
    }

    private String serializeEventos(){
        //eventos-1.v:evento1-x;evento2-y
        this.versions.get(this.zona).replace(3,this.versions.get(this.zona).get(3)+1);
        int versao = this.versions.get(this.zona).get(3);
        StringJoiner sj = new StringJoiner(";","eventos-"+this.zona+"."+versao+":","");
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
            verificaPercentagem();

            //Adiciona a ativos
            this.dispositivosAtivosCRDT.get(this.zona).add(id);
            return true;
        }

        if(msg.startsWith("evento")){
            String[] split = msg.split(":")[1].split(";");
            String id = split[0];
            if(split.length > 1){
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
            verificaPercentagem();

            //Remove dos ativos
            this.dispositivosAtivosCRDT.get(this.zona).remove(id);
            return true;
        }

        return false;
    }

    private void verificaPercentagem() {
        int total = getTotalDispositivos();
        if(total!=0){
            int percentagem = getTotalDispositivosZona() * 100 / total;
            int flag = 1;
            while (percentagem % 10 != 0) {
                percentagem--;
                flag = 0;
            }

            if(percentagem == 100){
                notificaAcimaDe(90);
            }
            else if (percentagem == 0){
                notificaAbaixoDe(10);
            }
            else{
                notificaAcimaDe(percentagem-10*flag);
                notificaAbaixoDe(percentagem+10);
            }
        }
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

    public void notifyNoDevicesTypeOnline(String tipo){
        this.toClient.send("online|Não existem dispositivos do tipo '"+ tipo+"' online.");
    }

    public void notifyRecordTipo(String tipo, int quant){
        this.toClient.send("record|Record de dispositivos do tipo '" + tipo + "' atingido ("+quant+" dispostivos)·");
    }

    public void notificaAcimaDe(int percentagem){
        while(percentagem >0){
            this.toClient.send(percentagem+"|Percentagem de dispositivos passou os "+percentagem+"%.");
            percentagem-=10;
        }
    }

    public void notificaAbaixoDe(int percentagem){
        while(percentagem < 100){
            this.toClient.send(percentagem+"|Percentagem de dispositivos desceu dos "+percentagem+"%.");
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
    private Map<Integer,Map<Integer,Integer>> versions;
    private ZMQ.Socket receive;
    private ZMQ.Socket inform;
    private ZMQ.Socket toClient;

    public UpdatesHandler(int zona, List<Integer> vizinhos, Map<Integer, Map<String, Set<String>>> dispositivosOnlineCRDT, Map<Integer, Set<String>> dispositivosAtivosCRDT, Map<Integer, Map<String, Integer>> totalEventosOcorridosCRDT, Map<Integer, Map<Integer, Integer>> versions, ZMQ.Socket receive, ZMQ.Socket inform, ZMQ.Socket toClient) {
        this.zona = zona;
        this.vizinhos = vizinhos;
        this.dispositivosOnlineCRDT = dispositivosOnlineCRDT;
        this.dispositivosAtivosCRDT = dispositivosAtivosCRDT;
        this.totalEventosOcorridosCRDT = totalEventosOcorridosCRDT;
        this.versions = versions;
        this.receive = receive;
        this.inform = inform;
        this.toClient = toClient;
    }

    public void run(){
        while(true){
            //online-1.v:tipo1->a,b;tipo2->c,d
            //ativos-1.v:a,b,c
            //eventos-1.v:evento1-x;evento2-y
            byte[] msg = this.receive.recv();
            String str = new String(msg);
            System.out.println("Received: "+str);
            String[] split = str.split(":");
            String[] tipoZona = split[0].split("-");
            int zona = Integer.parseInt(tipoZona[1].split("\\.")[0]);
            int versao = Integer.parseInt(tipoZona[1].split("\\.")[1]);
            System.out.println("Version "+versao+" from "+zona);
            //Caso seja a primeira vez que recebe algo da zona, cria entry no map com versão 0
            if(!this.versions.containsKey(zona)){
                this.versions.put(zona, new HashMap<>());
                for (int i = 1; i <= 3; i++) {
                    this.versions.get(zona).put(i, 0);
                }
            }
            switch (tipoZona[0]){
                case "online":
                    //Se versão for menor que a que esta no map, atualiza crdt e versão e envia para os vizinhos
                    if(this.versions.get(zona).get(1) < versao){
                        if(split.length>1){
                            mergeDispositivosOnlineCRDT(zona, deserializeOnline(split[1]));
                            System.out.println(str);
                            verificaPercentagem();
                            System.out.println("CRDT-Online: " + this.dispositivosOnlineCRDT);
                            this.versions.get(zona).replace(1,versao+1);
                            for(int ignored : vizinhos)
                                inform.send(str);
                        }
                    }
                    break;
                case "ativos":
                    if(this.versions.get(zona).get(2) < versao){
                        if(split.length>1){
                            mergeDispositivosAtivosCRDT(zona, deserializeAtivos(split[1]));
                        }else{
                            mergeDispositivosAtivosCRDT(zona, new HashSet<>());
                        }
                        System.out.println("CRDT-Ativos: " + this.dispositivosAtivosCRDT);
                        this.versions.get(zona).replace(2,versao+1);
                        for(int ignored : vizinhos)
                            inform.send(str);
                    }
                    break;
                case "eventos":
                    if(this.versions.get(zona).get(3) < versao){
                        if(split.length>1){
                            mergeEventosCRDT(zona, deserializeEventos(split[1]));
                        }
                        System.out.println("CRDT-Eventos: " + this.totalEventosOcorridosCRDT);
                        this.versions.get(zona).replace(3,versao+1);
                        for(int ignored : vizinhos)
                            inform.send(str);
                    }
                    break;
            }
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

    private void verificaPercentagem() {
        int total = getTotalDispositivos();
        if(total!=0){
            int percentagem = getTotalDispositivosZona() * 100 / total;
            int flag = 1;
            while (percentagem % 10 != 0) {
                percentagem--;
                flag = 0;
            }

            if(percentagem == 100){
                notificaAcimaDe(90);
            }
            else if (percentagem == 0){
                notificaAbaixoDe(10);
            }
            else{
                notificaAcimaDe(percentagem-10*flag);
                notificaAbaixoDe(percentagem+10);
            }
        }
    }

    public void notificaAcimaDe(int percentagem){
        while(percentagem >0){
            this.toClient.send(percentagem+"-Percentagem de dispositivos passou os "+percentagem+"%.");
            percentagem-=10;
        }
    }

    public void notificaAbaixoDe(int percentagem) {
        while (percentagem < 100) {
            this.toClient.send(percentagem + "-Percentagem de dispositivos desceu dos " + percentagem + "%.");
            percentagem += 10;
        }
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
}


class ClienteHandler extends Thread {
    private int zona;
    private Map<Integer,Map<String,Set<String>>> dispositivosOnlineCRDT;
    private Map<Integer,Set<String>> dispositivosAtivosCRDT;
    private Map<Integer,Map<String,Integer>> totalEventosOcorridosCRDT;
    private ZMQ.Socket fromClient;

    public ClienteHandler(int zona, Map<Integer, Map<String, Set<String>>> dispositivosOnlineCRDT, Map<Integer, Set<String>> dispositivosAtivosCRDT, Map<Integer, Map<String, Integer>> totalEventosOcorridosCRDT, ZMQ.Socket fromClient) {
        this.zona = zona;
        this.dispositivosOnlineCRDT = dispositivosOnlineCRDT;
        this.dispositivosAtivosCRDT = dispositivosAtivosCRDT;
        this.totalEventosOcorridosCRDT = totalEventosOcorridosCRDT;
        this.fromClient = fromClient;
    }

    public void run(){
        while (true){
            byte[] msg = fromClient.recv();
            System.out.println("Received from Client: "+new String(msg));
            String[] split = new String(msg).split(":");
            switch (split[0]){
                case "1" -> fromClient.send(onlineTipo(split[1]));
                case "2" -> fromClient.send(isOnline(split[1]));
                case "3" -> fromClient.send(dispositivosAtivos());
                case "4" -> fromClient.send(totalEventosTipo(split[1]));
            }
        }

    }

    public String onlineTipo(String tipo){
        int contador = 0;
        for(Map<String, Set<String>> entry: this.dispositivosOnlineCRDT.values())
            if(entry.containsKey(tipo))
                contador += entry.values().size();
            else
                contador += 0;
        return "Encontram-se "+contador+" dispositivos do tipo "+tipo+" online!";
    }

    public String isOnline(String dispositivo){
        for( Map<String, Set<String>> value : this.dispositivosOnlineCRDT.values())
            for(Set<String> dispositivos: value.values())
                if (dispositivos.contains(dispositivo))
                    return "O dispositivo de id "+dispositivo+" encontra-se online!";
        return "O dispositivo de id "+dispositivo+" não se encontra online!";
    }

    public String dispositivosAtivos(){
        int contador = 0;
        for(int key: this.dispositivosAtivosCRDT.keySet())
            contador += this.dispositivosAtivosCRDT.get(key).size();
        return "Existem " +contador+" dispositivos ativos de momento!";
    }

    public String totalEventosTipo(String tipo) {
        int total = 0;
        for(Map<String, Integer> entry : this.totalEventosOcorridosCRDT.values())
            if(entry.containsKey(tipo))
                total += entry.get(tipo);
        return "Ocorreu um total de " +total+ " eventos do tipo "+tipo+"!";
    }

}