package com.sharpwisdom.multithread;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Transaction;

import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

/**
 * 抢红包任务
 *
 * @author afei
 * @date 2016-08-15.
 */

public class RedPacketTask implements Callable<Double> {

    private static final Logger LOGGER = LoggerFactory.getLogger
            (RedPacketTask.class);


    /**
     * 缓存中的红包金额key
     */
    private static final String REDIS_REDPACKET_KEY = "redis::redpacket::key";

    /**
     * 缓存中的红包数量key
     */
    private static final String REDIS_REDPACKET_COUNT_KEY = "redis::redpacket:count::key";


    private JedisPool pool;

    /**
     * 线程门闩（计数器）
     */
    private CountDownLatch latch;
    private int chance;

    public void setLatch(CountDownLatch latch) {
        this.latch = latch;
    }

    public RedPacketTask(JedisPool pool) {
        this.pool = pool;
    }

    @Override
    public Double call() throws Exception {
        Jedis cli = this.pool.getResource();

        double redPacket = 0d;

        try {
            redPacket = getRedPacket(cli);
        } catch (Exception e) {
            LOGGER.info("出现异常{}", e.getMessage(), e);
            pool.returnBrokenResource(cli);
        } finally {
            pool.returnResource(cli);
            //latch.countDown();//计数器减一
            return redPacket;
        }

    }

    private double getRedPacket(Jedis cli) {
        cli.watch(REDIS_REDPACKET_KEY,REDIS_REDPACKET_COUNT_KEY);

        String id = UUID.randomUUID().toString();

        String countCache = cli.get(REDIS_REDPACKET_COUNT_KEY);
        int counter = Integer.valueOf(countCache);
        //LOGGER.info("count:{}",counter);
        //红包抢完了
        if (counter == 0) {
            LOGGER.info(Thread.currentThread().getName() + "红包抢完了");
            return 0d;
        }


        //还有得抢
        int remainCount = --counter;//红包个数减一

        String redPacketCache = cli.get(REDIS_REDPACKET_KEY);//剩下的红包金额
        Double remainingRedPacket = Double.valueOf(redPacketCache);
//        LOGGER.info("{}剩下的红包{}", Thread.currentThread().getName(), remainingRedPacket);


        Transaction tx = cli.multi();
        if (remainCount == 0) {//还剩下最后一个红包
            double money = (double) Math.round(remainingRedPacket * 100) / 100;
            LOGGER.info(Thread.currentThread().getName() + "还剩下{}个红包，恭喜你获得{}元", remainCount, money);

            --counter;//最后一个红包被抢

            //tx.set(REDIS_REDPACKET_KEY, String.valueOf(0d));//设置红包余额
            //tx.set(REDIS_REDPACKET_COUNT_KEY, String.valueOf(0));//没有红包用来抢了
            tx.incrByFloat(REDIS_REDPACKET_KEY,  - money);

            //剩余的数量
            //tx.set(REDIS_REDPACKET_COUNT_KEY, String.valueOf(remainCount));
            tx.incrBy(REDIS_REDPACKET_COUNT_KEY, (long) -1);

            //LOGGER.info("红包没有了");

            List<Object> exec = tx.exec();
            if (null == exec) {
                //LOGGER.info("===================================事务不完整");
            }else {
                LOGGER.info("111,id:{} is ok", id);
            }

            return money;
        }

        Random random = new Random();
        double min = 0.01; //最少0.01
        double max = remainingRedPacket / counter * 2;
        double money = random.nextDouble() * max;
        money = money <= min ? 0.01 : money;
        money = Math.floor(money * 100) / 100;

        //设置剩余的钱
        //tx.set(REDIS_REDPACKET_KEY, String.valueOf(remainingRedPacket - money));

        tx.incrByFloat(REDIS_REDPACKET_KEY,  - money);

        //剩余的数量
        //tx.set(REDIS_REDPACKET_COUNT_KEY, String.valueOf(remainCount));
        tx.incrBy(REDIS_REDPACKET_COUNT_KEY, (long) -1);
//        LOGGER.info("==================={}",Thread.currentThread().getName());

        List<Object> exec = tx.exec();
        if (null == exec || exec.isEmpty()) {
            /**
            try {
                Thread.sleep(300);
            } catch (Exception e) {
                e.printStackTrace();
            }
             */
            LOGGER.info("{}事务不完整,先休息一下{}",Thread.currentThread().getName(),exec);
            
            //这次没抢到，给它增加机会抢下次
            chance++;

            return 0d;
        }else {
            LOGGER.info("id:{} is ok", id);
        }

        LOGGER.info("{}获得红包{}", Thread.currentThread().getName(), money);
        //LOGGER.info("还剩下{}个红包", remainCount);

        return money;//返回抢到的红包
    }


}
