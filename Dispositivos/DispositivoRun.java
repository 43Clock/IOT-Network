package Dispositivos;

public class DispositivoRun {
    public static void main(String[] args) {
        if(args.length < 2) {
            System.out.println("Argumentos insuficientes!");
        } else {
            Dispositivo d = new Dispositivo(args[0],args[1]);
            d.start();
        }
    }
}
