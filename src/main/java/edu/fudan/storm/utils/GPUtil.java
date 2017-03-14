package edu.fudan.storm.utils;

import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.util.Arrays;

/**
 * Created by ybwang on 3/2/17.
 */
public class GPUtil {

    private static final int GPU_METRICS_INTERVAL_IN_SECS = 180;
    private static Jedis jedis = RedisUtil.getInstance();
    private static Logger LOG = Logger.getLogger(GPUtil.class);

    //this method get gpu id in loaclhost
    public static void writeGPUlist() throws IOException {

        String address = InetAddress.getLocalHost().getHostAddress();
        String gpuList = getGPUListName(address);
        jedis.del(gpuList);
        if(jedis.llen(gpuList)==0){
            Process pro = Runtime.getRuntime().exec(
                    new String[]{"nvidia-smi","-L"}
            );
            BufferedReader reader = new BufferedReader(new InputStreamReader(pro.getInputStream()));
            String out;
            while((out=reader.readLine())!=null){
                String gpuId = out.split(":")[0].split(" ")[1];
                jedis.rpush(gpuList, gpuId);
            }
            LOG.info("GPU on server "+address+ jedis.lrange(gpuList,0,-1));
        }
    }
    public static String getGPUListName(String address){
        return address+"_GPU";
    }

    public static void main(String[] args) throws IOException {
        writeGPUlist();
    }
}
