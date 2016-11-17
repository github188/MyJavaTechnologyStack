package test;

import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

/**群友错误的例子  源于对cas和LockSupport.park了解不够
 *  1.关于cas的@see 自己的MyLockSupport
 *  2. /**
 * Disables the current thread for thread scheduling purposes unless the
 * permit is available.
 *
 * <p>If the permit is available then it is consumed and the call
 * returns immediately; otherwise the current thread becomes disabled
 * for thread scheduling purposes and lies dormant until one of three
 * things happens:
 *
 * <ul>
 *
 * <li>Some other thread invokes {@link #unpark unpark} with the
 * current thread as the target; or
 *
 * <li>Some other thread {@linkplain Thread#interrupt interrupts}
 * the current thread; or
 *
 * <li>The call spuriously (that is, for no reason) returns.
 * </ul>
 *
 * <p>This method does <em>not</em> report which of these caused the
 * method to return. Callers should re-check the conditions which caused
 * the thread to park in the first place. Callers may also determine,
 * for example, the interrupt status of the thread upon return.
 *
    public static void park() {
        UNSAFE.park(false, 0L);
    }


 *
 *
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

    public static void main2(String[] args) {
        AtomicReference<Integer> test = new AtomicReference<>();
        System.out.println(test.compareAndSet(null, 10));
//        test.compareAndSet(10,null);
        System.out.println(test.compareAndSet(5,5));
        System.out.println(test.compareAndSet(10,5));

        System.out.println(test);

    }

    public static void main(String[] args) throws InterruptedException {
        TestLockSupport bl=new TestLockSupport();
        for(int i = 0; i < 5; i++){
            Thread t=new Thread(bl);
            t.start();

            t.join();//在这里加上join之后，相当于for循环第一次,第二次...是串行着进行的,因为for循环是在main线程执行的，main线程需要等待join 线程执行
            //而且加上join之后能成功是因为if (!sign.compareAndSet(null, current))一直为false，即不会有park出现，而如果多次unpark，只有一次park也不会出现什么问题，结果是许可处于可用状态。
        }
        Thread.currentThread().sleep(5000);

        System.out.println(sum);
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