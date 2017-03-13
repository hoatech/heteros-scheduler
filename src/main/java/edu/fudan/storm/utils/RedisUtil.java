package edu.fudan.storm.utils;

import redis.clients.jedis.Jedis;

/**
 * Created by ybwang on 2/23/17.
 */
public class RedisUtil {
    private static Jedis client;

    public static Jedis getInstance() {
        if(client==null) {
            client = new Jedis("10.134.142.114", 6379);
            client.auth("admin");
        }
        return client;
    }
    public static String getTaskCPULoadMap(String topologyId){
        return topologyId+"_cpu_load";
    }
    public static String getTaskGPULoadMap(String topologyId){
        return topologyId+"_gpu_load";
    }
    public static String getTaskToComponentMap(String topologyId){
        return topologyId+"_task_to_component";
    }

}
