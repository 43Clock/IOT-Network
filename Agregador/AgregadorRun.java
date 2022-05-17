package Agregador;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

class AgregadorRun {
    public static void main(String[] args) {
        if(args.length < 1) {
            System.out.println("Argumentos insuficientes!");
        } else {
            Set<String> dispositivosOnline = new HashSet<String>();
            Map<String,Set<String>> tipos = new HashMap<String,Set<String>>();
            if(args.length == 2){
                Agregador a = new Agregador(dispositivosOnline,tipos,args[0],args[1]);
                a.start();
            }
            else if(args.length == 1){
                AgregadorSuper a = new AgregadorSuper(dispositivosOnline,tipos,args[0]);
                a.run();
                a.start();
            }
        }
    }
}