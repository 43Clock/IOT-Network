package Cliente;

public class ClienteRun {
    public static void main(String[] args) {
        if(args.length < 1){
            System.out.println("Argumentos insuficientes");
        } else {
            Cliente c = new Cliente(Integer.parseInt(args[0]));
            c.start();
        }
    }
}
