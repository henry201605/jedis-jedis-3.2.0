package nci.henry;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.*;

import java.util.Arrays;
import java.util.List;

/**
 * @Author: henry
 * @Date: 2020/4/18 15:43
 * @Description: 测试ShardedJedis
 */
public class JedisShardedTest {

    private ShardedJedisPool shardedJedisPool;
    private ShardedJedis shardedJedis;

    @Before
    public void initJedis(){
        JedisPoolConfig poolConfig = new JedisPoolConfig();

        // Redis服务器
        JedisShardInfo shardInfo1 = new JedisShardInfo("132.232.115.96", 9527);
        JedisShardInfo shardInfo2 = new JedisShardInfo("132.232.115.96", 9526);

        // 连接池
        List<JedisShardInfo> infoList = Arrays.asList(shardInfo1, shardInfo2);
        shardedJedisPool = new ShardedJedisPool(poolConfig, infoList);
    }

    @Test
    public void testSet(){
        try {
            shardedJedis = shardedJedisPool.getResource();
            for (int i = 0; i < 100; i++) {
                shardedJedis.set("k" + i, "" + i);
                Client client = shardedJedis.getShard("k"+i).getClient();
                System.out.println("set值："+i+ "，到：" + client.getHost() + ":" + client.getPort());
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testGet(){
        try{
            shardedJedis = shardedJedisPool.getResource();
            for(int i=0; i<100; i++){
                String value = shardedJedis.get("k" + i);
                Client client = shardedJedis.getShard("k"+i).getClient();
                System.out.println("取到值："+ value +"，"+"当前key位于：" + client.getHost() + ":" + client.getPort());
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @After
    public void close(){
        if(shardedJedis!=null) {
            shardedJedis.close();
        }
    }
}
