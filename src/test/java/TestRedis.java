import edu.fudan.storm.utils.RedisUtil;
import redis.clients.jedis.BinaryClient;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by ybwang on 3/2/17.
 */
public class TestRedis {
    public static void main(String[] args){
        Jedis jedis = RedisUtil.getInstance();
        System.out.println("Flush DB: "+jedis.flushDB());
        Map<String, String> map = new HashMap<>();
        map.put("field","ily");
        jedis.hmset("ybwang", map);
        map.clear();
        map.put("field1","i love you");
        jedis.hmset("ybwang", map);
        List<String> test = jedis.hmget("ybwang","field","field1");
        Set<String> test1 = jedis.hkeys("ybwang1");
        System.out.println(test+""+test1);
    }
}
