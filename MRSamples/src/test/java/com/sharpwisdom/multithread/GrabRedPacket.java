package com.sharpwisdom.multithread;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * 利用redis的乐观锁事务，实现并发抢红包
 *
 * @author afei
 * @date 2016-08-15.
 */

public class GrabRedPacket {

    private static final Logger LOGGER = LoggerFactory.getLogger
            (RedPacketTask.class);

    private static final String HOST = "172.16.72.144";
    private static final int PORT = 6379;

    private static JedisPoolConfig config;
    private static JedisPool REDIS_POOL;
    private static ExecutorService service;

    /**
     * 缓存中的红包金额key
     */
    private static final String REDIS_REDPACKET_KEY = "redis::redpacket::key";
    /**
     * 缓存中的红包数量key
     */
    private static final String REDIS_REDPACKET_COUNT_KEY = "redis::redpacket:count::key";

    /**
     * 100个人同时来抢红包
     */
    private static int THREAD_TOGETHER_NUMS = 100;

    private static CountDownLatch latch;

    static {

        //利用Redis连接池，保证多个线程利用多个连接，充分模拟并发性
        config = new JedisPoolConfig();
        config.setMaxIdle(100);
        config.setMaxWaitMillis(1000);
        config.setMaxTotal(100);
        config.setTestOnBorrow(true);
        REDIS_POOL = new JedisPool(config, HOST, PORT);

        //利用ExecutorService 管理线程
        service = Executors.newFixedThreadPool(THREAD_TOGETHER_NUMS);

        //CountDownLatch保证主线程在所有线程结束之后退出
        latch = new CountDownLatch(THREAD_TOGETHER_NUMS);

    }

    public static void main(String args[]) {
        Jedis cli = REDIS_POOL.getResource();
        try{
            cli.del(REDIS_REDPACKET_KEY);//先删除既定的key
            cli.del(REDIS_REDPACKET_COUNT_KEY);//先删除既定的key
            cli.set(REDIS_REDPACKET_KEY, String.valueOf(10d));//设定默认值10元
            cli.set(REDIS_REDPACKET_COUNT_KEY, String.valueOf(10));//100个人只有10个人能抢到

            List<Future<Double>> futureList = new ArrayList<>();

            //main线程中开 THREAD_TOGETHER_NUMS 个子线程
            for (int i = 0; i < 1000; i++) {
                RedPacketTask task = new RedPacketTask(REDIS_POOL);
                task.setLatch(latch);

                Future<Double> stringFuture = service.submit(task);
                futureList.add(stringFuture);
                /**
                 try {
                 Double red = stringFuture.get();
                 LOGGER.info("打开的红包消息是,{}", red);

                 } catch (InterruptedException e) {
                 LOGGER.info("出现异常{}", e.getMessage(), e);
                 } catch (ExecutionException e) {
                 LOGGER.info("出现异常{}", e.getMessage(), e);
                 }
                 */

            }

            for (Future<Double> future : futureList) {
                try {
                    future.get();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            }

            service.shutdown();

            String red = cli.get(REDIS_REDPACKET_KEY);
            REDIS_POOL.returnResource(cli);
            LOGGER.info("all sub thread succeed !红包还剩下 {}", red);
        }catch (Exception e){
            REDIS_POOL.returnBrokenResource(cli);
        }
    }

}
