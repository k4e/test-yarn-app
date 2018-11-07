package kitade.coins;

import java.io.IOException;

public class SleepingWorker {
    
    public static int DEFAULT_SEC = 120;
    
    public static void main(String[] args) throws IOException, InterruptedException {
        int sec;
        if(args.length >= 1) {
            sec = Integer.parseInt(args[0]);
        }
        else {
            sec = DEFAULT_SEC;
        }
        
        for(int i = 0; i < sec; ++i) {
            if(i % 10 == 0) {
                System.out.println(String.format("%d sec.", i));
            }
            Thread.sleep(1000);
        }
    }
}
