package edu.fudan.storm.utils;

import jcuda.driver.CUdevice;
import jcuda.driver.JCudaDriver;
import redis.clients.jedis.Jedis;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;

import static jcuda.driver.JCudaDriver.*;

/**
 * Created by ybwang on 3/2/17.
 */
public class GPUtil {

    //this method get gpu uuid in loaclhost
    public static void writeGPUlist() throws IOException {

        Jedis jedis = RedisUtil.getInstance();
        String address = InetAddress.getLocalHost().getHostAddress();
        String gpuList = getGPUListName(address);
        jedis.del(gpuList);
        Process pro = Runtime.getRuntime().exec(
                new String[]{"nvidia-smi","-q"}
        );
        BufferedReader reader = new BufferedReader(new InputStreamReader(pro.getInputStream()));
        String out;
        while((out=reader.readLine())!=null){
            if(out.contains("GPU UUID")){
                String[] uuid = out.split(": ");
                jedis.rpush(gpuList,uuid[1]);
            }
        }
//        System.out.println(jedis.lrange(gpuList,0,-1));
    }
    public static String getGPUListName(String address){
        return address+"_GPU";
    }
    public static void main(String[] args) throws IOException {
        writeGPUlist();
    }
}
