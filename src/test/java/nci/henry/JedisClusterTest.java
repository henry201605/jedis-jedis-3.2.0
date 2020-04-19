package nci.henry;

import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisSentinelPool;

import java.io.IOException;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

/**
 * @Author: henry
 * @Date: 2020/4/18 22:43
 * @Description: 测试Cluster
 */
public class JedisClusterTest {


    private JedisCluster cluster;

    @Before
    public void initJedis(){
        // 不管是连主备，还是连几台机器都是一样的效果
        HostAndPort hap1 = new HostAndPort("132.232.115.96",6373);
        HostAndPort hap2 = new HostAndPort("132.232.115.96",6374);
        HostAndPort hap3 = new HostAndPort("132.232.115.96",6375);
        HostAndPort hap4 = new HostAndPort("132.232.115.96",6376);
        HostAndPort hap5 = new HostAndPort("132.232.115.96",6377);
        HostAndPort hap6 = new HostAndPort("132.232.115.96",6378);

        Set nodes = new HashSet<HostAndPort>();
        nodes.add(hap1);
        nodes.add(hap2);
        nodes.add(hap3);
        nodes.add(hap4);
        nodes.add(hap5);
        nodes.add(hap6);

        cluster = new JedisCluster(nodes);
    }

    @Test
    public void testGet(){
        try {
            String key = "cluster:henry";
            cluster.set(key, "henry2016");
            System.out.println("获取集群中" + key + "的值--->"+cluster.get(key));;
            cluster.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
