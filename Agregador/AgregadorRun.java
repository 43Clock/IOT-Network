package Agregador;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

class AgregadorRun {
    public static void main(String[] args) {
        if(args.length < 2) {
            System.out.println("Argumentos insuficientes!");
        } else {
            ArrayList<Integer> list = new ArrayList<Integer>();
            for(int i = 1;i<args.length;i++){
                list.add(Integer.parseInt(args[i]));
            }
            Agregador a = new Agregador(args[0],list);
            a.start();
        }
    }
}