package test;

import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

/**
 * @author wuys
 * @date 2016/11/16.
 */
public class TestLockSupport implements Runnable{
    private AtomicReference<Thread> sign = new AtomicReference<>();
    // 获取锁
    public void lock() {
        Thread current = Thread.currentThread();
        System.out.println(current.getName() + "进入lock!");
        if (!sign.compareAndSet(null, current)){ //许可默认是被占用的
            System.out.println(current.getName() + "获得锁!");
            LockSupport.park();// 获取许可
        }
    }



    public AtomicReference<Thread> getSign() {
        return sign;
    }

    // 释放锁
    public void unlock() {
        Thread current = Thread.currentThread();
        System.out.println(current.getName() + "进入unlock!");
        sign.compareAndSet(current, null);
        LockSupport.unpark(current);//释放许可
        System.out.println(current.getName() + "释放锁!");
    }
    volatile static int sum =0;
    @Override
    public void run() {
        lock();
        sum++;
        System.out.println(Thread.currentThread().getName()+"--"+sum);
        unlock();
    }

    public static void main(String[] args) {
        AtomicReference<Integer> test = new AtomicReference<>();
        System.out.println(test.compareAndSet(null, 10));
//        test.compareAndSet(10,null);
        System.out.println(test.compareAndSet(5,5));
        System.out.println(test.compareAndSet(10,5));

        System.out.println(test);

    }

    public static void main1(String[] args) throws InterruptedException {
        TestLockSupport bl=new TestLockSupport();
        for(int i = 0; i < 5; i++){
            Thread t=new Thread(bl);
            t.start();

//            t.join();
        }
        Thread.currentThread().sleep(5000);

        System.out.println(sum);
        System.out.println("========" + bl.getSign().get()==null?null:bl.getSign().get().getName());
//        AtomicReference<Integer> test = new AtomicReference<>();
//        test.compareAndSet(null,10);
////        test.compareAndSet(10,null);
//        System.out.println(test);
    }

}
/**
 *
 * Thread-3进入lock!
 Thread-3--1
 Thread-3进入unlock!
 Thread-0进入lock!
 Thread-0--2
 Thread-0进入unlock!
 Thread-3释放锁!
 Thread-4进入lock!
 Thread-4--3
 Thread-4进入unlock!
 Thread-4释放锁!
 Thread-2进入lock!
 Thread-0释放锁!
 Thread-1进入lock!
 Thread-1获得锁!
 Thread-2--4
 Thread-2进入unlock!
 Thread-2释放锁!
 4
 *
 *
 *
 **/