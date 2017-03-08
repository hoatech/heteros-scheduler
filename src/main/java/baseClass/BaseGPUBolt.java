package baseClass;

import edu.fudan.storm.utils.GPUtil;
import edu.fudan.storm.utils.RedisUtil;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import redis.clients.jedis.Jedis;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.net.InetAddress;

/**
 * Created by ybwang on 3/1/17.
 */
public class BaseGPUBolt extends BaseBasicBolt implements IRichBolt{

    private String IpAddress;
    private String GPUUUID;
    private Jedis jedis;
    //Thread gpu load metrics(10-100)
    private int gpuLoad;
    private String getGPUUUID(String IpAddress){
        String gpuListName = GPUtil.getGPUListName(IpAddress);
        String GPUUUID = this.jedis.rpop(gpuListName);
        this.jedis.lpush(gpuListName, GPUUUID);
        return  GPUUUID;
    }
    public BaseGPUBolt(int gpuLoad){
        this.gpuLoad=gpuLoad;
    }
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.jedis = RedisUtil.getInstance();
        try {
            this.IpAddress = InetAddress.getLocalHost().getHostAddress();
            this.GPUUUID = getGPUUUID(IpAddress);
            String gpu_load_map = RedisUtil.getTaskGPULoadMap(topologyContext.getStormId());
            Map<String, String> gpuLoadMap = new HashMap<>();
            gpuLoadMap.put(String.valueOf(topologyContext.getThisTaskId()),String.valueOf(gpuLoad));
            jedis.hmset(gpu_load_map, gpuLoadMap);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple tuple) {

    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
