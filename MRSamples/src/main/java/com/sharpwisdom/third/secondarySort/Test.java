package com.sharpwisdom.third.secondarySort;

/**
 * Created by wuys on 2016/8/17.
 */
public class Test extends Thread{

    private volatile boolean stop = false;

    public void stopMe(){
        stop = true;
    }

    @Override
    public void run() {
        int i = 0;
        while(!stop){
            i ++ ;
        }
        System.out.println(i + "Stop thread");
    }

    public static void main(String[] args) throws InterruptedException {
        Test test = new Test();
        test.start();
        Thread.sleep(100);
        test.stopMe();
    }
}
