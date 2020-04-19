package nci.henry;

import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.JedisSentinelPool;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;

/**
 * @Author: henry
 * @Date: 2020/4/18 18:43
 * @Description: 测试Sentinel
 */
public class JedisSentinelTest {


    private JedisSentinelPool pool;

    @Before
    public void initJedis(){
        // master的名字是sentinel.conf配置文件里面的名称
        String masterName = "mymaster";
        Set<String> sentinels = new HashSet<String>();
        sentinels.add("132.232.115.96:16380");
        sentinels.add("132.232.115.96:16381");
        sentinels.add("132.232.115.96:16382");
        pool = new JedisSentinelPool(masterName, sentinels);
    }

    @Test
    public void testSet(){
        try {
            pool.getResource().set("henry", "time:" + new Date());
            System.out.println(pool.getResource().get("henry"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
