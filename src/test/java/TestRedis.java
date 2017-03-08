import edu.fudan.storm.utils.RedisUtil;
import redis.clients.jedis.BinaryClient;
import redis.clients.jedis.Jedis;

import java.util.List;

/**
 * Created by ybwang on 3/2/17.
 */
public class TestRedis {
    public static void main(String[] args){
        Jedis jedis = RedisUtil.getInstance();
        System.out.println("Flush DB: "+jedis.flushDB());;
        System.out.println("==List==");
        // Çå¿ÕÊý¾Ý
        //Ìí¼ÓÊý¾Ý
        jedis.rpush("names", "ÌÆÉ®");
        jedis.rpush("names", "Îò¿Õ");
        jedis.rpush("names", "°Ë½ä");
        jedis.rpush("names", "Îò¾»");
        // ÔÙÈ¡³öËùÓÐÊý¾Ýjedis.lrangeÊÇ°´·¶Î§È¡³ö£¬
        // µÚÒ»¸öÊÇkey£¬µÚ¶þ¸öÊÇÆðÊ¼Î»ÖÃ£¬µÚÈý¸öÊÇ½áÊøÎ»ÖÃ£¬jedis.llen»ñÈ¡³¤¶È -1±íÊ¾È¡µÃËùÓÐ
        List<String> values = jedis.lrange("names", 0, -1);
        System.out.println(values);

        jedis.lpush("scores", "100");
        jedis.lpush("scores", "99");
        jedis.lpush("scores", "55");
        // Êý×é³¤¶È
        System.out.println(jedis.llen("scores"));
        // ÅÅÐò
        System.out.println(jedis.sort("scores"));
        // ×Ö´®
        System.out.println(jedis.lrange("scores", 0, 3));
        // ÐÞ¸ÄÁÐ±íÖÐµ¥¸öÖµ
        jedis.lset("scores", 0, "66");
        // »ñÈ¡ÁÐ±íÖ¸¶¨ÏÂ±êµÄÖµ
        System.out.println(jedis.lindex("scores", 1));
        // É¾³ýÁÐ±íÖ¸¶¨ÏÂ±êµÄÖµ
        System.out.println(jedis.lrem("scores", 1, "99"));
        // É¾³ýÇø¼äÒÔÍâµÄÊý¾Ý
        System.out.println(jedis.ltrim("scores", 0, 1));
        // ÁÐ±í³öÕ»
        System.out.println(jedis.lpop("scores"));
        // Õû¸öÁÐ±íÖµ
        System.out.println(jedis.lrange("scores", 0, -1));
        //ÔÚ100Ö®Ç°Ìí¼ÓÊý¾ÝÊý¾Ý
        jedis.linsert("scores", BinaryClient.LIST_POSITION.BEFORE, "100", "22");
        //ÔÚ100Ö®ºóÌí¼ÓÊý¾ÝÊý¾Ý
        jedis.linsert("scores", BinaryClient.LIST_POSITION.AFTER, "100", "77");
        // Õû¸öÁÐ±íÖµ
        System.out.println(jedis.lrange("scores", 0, -1));
        //°ÑListÖÐkeyÎªscoresµÄµÚ¶þÌõÊý¾Ý¸ÄÎª88
        jedis.lset("scores", 1, "88");
        // Õû¸öÁÐ±íÖµ
        System.out.println(jedis.lrange("scores", 0, -1));
        //½ØÈ¡ÁÐ±íÇø¼äÄÚµÄÔªËØ
        jedis.ltrim("scores", 1, 2);
        // Õû¸öÁÐ±íÖµ
        System.out.println(jedis.lrange("scores", 0, -1));
        //½«Öµ value ²åÈëµ½ÁÐ±í key µÄ±íÎ²£¬µ±ÇÒ½öµ± key ´æÔÚ²¢ÇÒÊÇÒ»¸öÁÐ±í¡£µ± key ²»´æÔÚÊ±£¬ RPUSHX ÃüÁîÊ²Ã´Ò²²»×ö¡£
        jedis.rpushx("name", "Alex");
        System.out.println(jedis.lrange("name", 0, -1));
    }
}
