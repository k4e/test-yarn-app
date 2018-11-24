package k4e.coins.tsukuba;

import java.io.IOException;

public class SleepingWorker {
    
    public static int DEFAULT_SEC = 120;
    
    public static void main(String[] args) throws IOException, InterruptedException {
        final int sec = (args.length == 0 ? DEFAULT_SEC : Integer.parseInt(args[0]));
        
        System.out.println("Begin.");
        for(int i = 0; i <= sec; ++i) {
            if(i % 10 == 0) {
                System.out.println(String.format("%d sec.", i));
            }
            Thread.sleep(1000);
        }
        System.out.println("End.");
    }
}
