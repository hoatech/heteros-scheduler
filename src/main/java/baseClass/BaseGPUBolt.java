package baseClass;

import edu.fudan.storm.utils.GPUtil;
import edu.fudan.storm.utils.RedisUtil;
import jcuda.driver.CUcontext;
import jcuda.driver.CUdevice;
import jcuda.driver.JCudaDriver;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import redis.clients.jedis.Jedis;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.net.InetAddress;

import static jcuda.driver.JCudaDriver.cuCtxCreate;
import static jcuda.driver.JCudaDriver.cuDeviceGet;
import static jcuda.driver.JCudaDriver.cuInit;

/**
 * Created by ybwang on 3/1/17.
 */
public abstract class BaseGPUBolt extends BaseRichBolt{

    private Jedis jedis;
    /*
    Thread gpu load metrics(10-100)
     */
    private int gpuLoad;

    private void initCUDAContext() throws UnknownHostException {
        String IpAddress = InetAddress.getLocalHost().getHostAddress();
        String gpuListName = GPUtil.getGPUListName(IpAddress);
        String GPUID = this.jedis.rpop(gpuListName);
        this.jedis.lpush(gpuListName, GPUID);
        // Enable exceptions and omit all subsequent error checks
        JCudaDriver.setExceptionsEnabled(true);
        // Initialize the driver and create a context for the first device.
        cuInit(0);
        CUcontext pctx = new CUcontext();
        CUdevice dev = new CUdevice();
        cuDeviceGet(dev, 0);
        cuCtxCreate(pctx, 0, dev);
    }
    public BaseGPUBolt(int gpuLoad){
        this.gpuLoad=gpuLoad;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.jedis = RedisUtil.getInstance();
        try {
            /*
            write gpu load into redis
             */
            String gpu_load_map = RedisUtil.getTaskGPULoadMap(topologyContext.getStormId());
            Map<String, String> gpuLoadMap = new HashMap<>();
            gpuLoadMap.put(String.valueOf(topologyContext.getThisTaskId()),String.valueOf(gpuLoad));
            jedis.hmset(gpu_load_map, gpuLoadMap);
            /*
            initial cuda context
             */
            initCUDAContext();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }
}
