package kitade.coins;

import java.io.IOException;

public class Worker {

    public static void main(String[] args) throws IOException, InterruptedException {
        for(int i = 0; i < 60; ++i) {
            if(i % 5 == 0) {
                System.out.println(String.format("%d sec.", i));
            }
            Thread.sleep(1000);
        }
    }
}
