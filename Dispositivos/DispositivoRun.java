package Dispositivos;

public class DispositivoRun {
    public static void main(String[] args) {
        if(args.length < 4) {
            System.out.println("Argumentos insuficientes!");
        } else {
            Dispositivo d = new Dispositivo(args[0],args[1],args[2],args[3]);
            d.start();
        }
    }
}
