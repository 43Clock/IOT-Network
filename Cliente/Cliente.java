package Cliente;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.nio.charset.StandardCharsets;
import java.util.*;

public class Cliente {

    private ZMQ.Socket receive;
    private ZMQ.Socket toAgregador;
    private int zona;
    private List<String> notif;
    private Map<Integer,Boolean> tiposNotificados;

    public Cliente(int zona) {
        this.zona = zona;
        this.notif = new ArrayList<>();
        this.tiposNotificados = new HashMap<>();
        this.tiposNotificados.put(1,false);
        this.tiposNotificados.put(2,false);
        for(int i = 10;i<100;i+=10)
            this.tiposNotificados.put(i,false);
    }

    public void start() {
        try (ZContext context = new ZContext();
             ZMQ.Socket receive = context.createSocket(SocketType.SUB);
             ZMQ.Socket toAgregador = context.createSocket(SocketType.REQ))
        {
            this.receive = receive;
            this.toAgregador = toAgregador;
            this.receive.connect("tcp://localhost:" + (3004 + 100 * zona));
            this.toAgregador.connect("tcp://localhost:" + (3005 + 100 * zona));
            ListenToSubs ls = new ListenToSubs(this.zona,this.receive,this.notif);
            ls.start();
            navigateMenu();
        }
    }

    private void navigateMenu() {
        int currentMenu = 1;
        boolean flag = true;
        Scanner scanner = new Scanner(System.in);
        while (flag) {
            clearScreen();
            imprimeMenu(currentMenu);
            int opcao = scanner.nextInt();
            switch (currentMenu){
                case 1:
                    switch (opcao) {
                        case 1 -> currentMenu = 2;
                        case 2 -> currentMenu = 3;
                        case 3 -> currentMenu = 4;
                        case 4 -> currentMenu = 5;
                        case 5 -> flag = false;
                        default -> System.out.println("Opção inválida!");
                    }
                    break;
                case 2:
                    switch (opcao) {
                        case 3 -> sendRequest(opcao);
                        case 1, 2, 4 -> {
                            clearScreen();
                            if (opcao == 1) {
                                System.out.print("Escolher tipo de dispositivo: ");
                            }
                            else if (opcao == 2){
                                System.out.print("Escolher dispositivo para verificar se está online: ");
                            }
                            else {
                                System.out.print("Escolher tipo de evento: ");
                            }

                            Scanner s = new Scanner(System.in);
                            String valor = s.nextLine();
                            sendRequest(opcao, valor);
                        }
                        case 5 -> currentMenu = 1;
                        default -> System.out.println("Opção inválida");
                    }
                    currentMenu = 1;
                    break;
                case 3:
                    switch (opcao){
                        case 1,2 -> subscrever(opcao);
                        case 3 -> {
                            while (true){
                                clearScreen();
                                System.out.print("Escolher percentagem a ser notificada: ");
                                int valor = scanner.nextInt();
                                if(valor % 10 == 0 && valor > 0 && valor <100){
                                    subscrever(valor);
                                    break;
                                }
                            }
                        }
                        case 4 -> currentMenu = 1;
                        default -> System.out.println("Opção inválida");
                    }
                    currentMenu = 1;
                    break;
                case 4:
                    switch (opcao){
                        case 1,2 -> notSubscrever(opcao);
                        case 3 -> {
                            while (true){
                                clearScreen();
                                System.out.print("Escolher percentagem a ser removida das notificações: ");
                                Scanner s = new Scanner(System.in);
                                int valor = s.nextInt();
                                if(valor % 10 == 0 && valor > 0 && valor <100){
                                    notSubscrever(valor);
                                    break;
                                }
                            }
                        }
                        case 4 -> currentMenu = 1;
                        default -> System.out.println("Opção inválida");
                    }
                    currentMenu = 1;
                    break;
                case 5:
                    switch (opcao){
                        case 1:
                            break;
                        case 2:
                            currentMenu = 1;
                            break;
                        default:
                            System.out.println("Opção inválida");
                            break;
                    }
                    break;
                default:
                    System.out.println("Opção inválida");
                    break;
            }
        }
    }

    private void subscrever(int i) {
        String s;
        switch (i){
            case 1 -> s = "online";
            case 2 -> s = "record";
            default -> s = ""+i;
        }
        if(!this.tiposNotificados.get(i)){
            this.receive.subscribe(s.getBytes(StandardCharsets.UTF_8));
            this.tiposNotificados.replace(i,true);
        }
    }

    private void notSubscrever(int i){
        String s;
        switch (i){
            case 1 -> s = "online";
            case 2 -> s = "record";
            default -> s = ""+i;
        }
        if(this.tiposNotificados.get(i)){
            this.receive.unsubscribe(s.getBytes(StandardCharsets.UTF_8));
            this.tiposNotificados.replace(i,false);
        }
    }

    private void imprimeMenu(int menu){
        switch (menu){
            case 1:
                System.out.println("\t-------Zona "+this.zona+"-------");
                System.out.println("1 - Fazer pedidos ao agregador.");
                System.out.println("2 - Escolher notificações desejadas.");
                System.out.println("3 - Remover subscrição de notificações.");
                System.out.println("4 - Ver notificações recebidas.");
                System.out.println("5 - Sair.");
                break;
            case 2:
                System.out.println("\t-------Fazer pedidos ao agregador-------");
                System.out.println("1 - Numero de dispositivos de um dado tipo online.");
                System.out.println("2 - Verificar se dispositivo se encontra online no sistema.");
                System.out.println("3 - Numero de dispositivos ativos no sistema.");
                System.out.println("4 - Numero de eventos de um certo tipo ocorridos no sistema.");
                System.out.println("5 - Voltar.");
                break;
            case 3,4:
                if(menu == 3)
                    System.out.println("\t-------Escolher notificações desejadas-------");
                else
                    System.out.println("\t-------Remover subscrição de notificações-------");
                System.out.println("1 - Notificar quando não existem mais dispositivos online de um certo tipo.");
                System.out.println("2 - Notificar quando é atingido algum record de dispositivos online, por tipo.");
                System.out.println("3 - Notificar quando uma certa percentagens de dispositivos se encontra na zona.");
                System.out.println("4 - Voltar.");
                break;
            case 5:
                System.out.println("\t-------Histórico de notificações-------");
                for (int i = 0; i < this.notif.size(); i++) {
                    System.out.println(i+1+". "+this.notif.get(i));
                }
                System.out.println("1 - Refresh");
                System.out.println("2 - Voltar");
                break;
        }
        System.out.print("\nEscolher opção: ");
    }

    public void clearScreen() {
        System.out.print("\033[H\033[2J");
        System.out.flush();
    }


    private void sendRequest(int opcao) {
        this.toAgregador.send("" + opcao);
        byte[] msg = toAgregador.recv();
        String str = new String(msg);
        System.out.println(str);
        Scanner s = new Scanner(System.in);
        s.nextLine();
    }

    private void sendRequest(int opcao,String valor) {
        this.toAgregador.send(opcao+":"+valor);
        byte[] msg = toAgregador.recv();
        String str = new String(msg);
        System.out.println(str);
        Scanner s = new Scanner(System.in);
        s.nextLine();
    }

}

class ListenToSubs extends Thread {
    private int zona;
    private ZMQ.Socket receive;
    private List<String> notif;

    public ListenToSubs(int zona, ZMQ.Socket receive, List<String> notif) {
        this.zona = zona;
        this.receive = receive;
        this.notif = notif;
    }

    public void run(){
        while (true){
            byte[] msg = receive.recv();
            String str = new String(msg);
            String[] split = str.split("\\|");
            this.notif.add(split[1]);
        }
    }

}