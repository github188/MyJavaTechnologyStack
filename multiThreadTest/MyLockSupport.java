package test;

import java.util.concurrent.atomic.AtomicReference;

/**
 *
 * @author wuys
 * @date 2016/11/16.
 */
public class MyLockSupport implements Runnable {

    private final Object SIGN = new Object();
    AtomicReference<Object> lock = new AtomicReference<>(SIGN);

    public void lock(){
        for(;;){
            if(lock.compareAndSet(SIGN,Thread.currentThread())){//如果lock中放的还是SIGN的话，此处为true,然后里面的值会被改成当前的thread
                break;
            }else {
                continue;
            }
        }
    }

    public void unlock(){
        for(;;){
            if(lock.compareAndSet(Thread.currentThread(),SIGN)){//如果lock中放的是当前线程，此处为true，并会将SIGN放会lock中去
                break;
            }else {
                continue;
            }
        }
    }

    volatile static int sum =0;

    @Override
    public void run() {
        lock();
        sum++;
        System.out.println(Thread.currentThread().getName()+"--"+sum);
        unlock();
    }

    public static void main(String[] args) throws InterruptedException {
        MyLockSupport bl=new MyLockSupport();
        for(int i = 0; i < 1000; i++){
            Thread t=new Thread(bl);
            t.start();

//            t.join();
        }
        Thread.currentThread().sleep(5000);

        System.out.println(sum);
    }
}
