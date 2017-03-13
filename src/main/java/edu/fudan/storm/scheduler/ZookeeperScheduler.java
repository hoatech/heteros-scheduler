package edu.fudan.storm.scheduler;

import com.google.gson.Gson;
import edu.fudan.storm.units.SupervisorSlot;
import edu.fudan.storm.utils.RedisUtil;
import edu.fudan.storm.utils.Stootils;
import org.apache.storm.scheduler.*;
import org.apache.storm.shade.org.apache.curator.framework.CuratorFramework;
import org.apache.storm.shade.org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.io.*;
import java.util.*;

/**
 * This scheduler reads the schedule from Zookeeper. It uses the even scheduler to schedule all the topologies
 * for which is doesn't find a schedule in Zookeeper.
 * <p/>
 * You can tell storm to use this scheduler by doing the following:
 * <ol>
 * <li>Put the jar containing this scheduler into <b>$STORM_HOME/lib on the nimbus</b> server.</li>
 * <li>Set the following configuration parameter: storm.scheduler: "ZookeeperScheduler"</li>
 * </ol>
 * <p/>
 * The schedules are expected to be written into Zookeeper znodes having the names of the topology-ids under  either
 * {@link #DEFAULT_SCHEDULING_SCHEDULES_ZK_PATH} (value = {@value #DEFAULT_SCHEDULING_SCHEDULES_ZK_PATH}) or a path that
 * is configured as a property in the storm config object {@link #CONF_SCHEDULING_SCHEDULES_ZK_PATH} (value =
 * {@value #CONF_SCHEDULING_SCHEDULES_ZK_PATH}).
 * <p/>
 * <b>Please Note #1:</b> This scheduler assumes that there is task per executor thread. If there are multiple tasks
 * assigned per executor, the scheduler will throw an exception. However, multiple executor threads can be assigned
 * to one single worker.
 * <p/>
 * <b>Please Note #2:</b> This scheduler will ignore the configured number of workers per topology configured in
 * {@link org.apache.storm.Config#TOPOLOGY_WORKERS} as it merely applies the configured schedule it finds in Zookeeper.
 * The partitions it finds configured will be applied, treating supervisors as partitions. This means that the tasks
 * that are assigned to a partition will be distributed among all workers that are running on a given supervisor.
 * This is easiest to reason about if there is exactly one supervisor per host and one worker (slot) per supervisor.
 * If multiple workers per supervisor, the tasks will be distributed evenly among all workers of each supervisor.
 *
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 */
public class ZookeeperScheduler implements IScheduler {

    /**
     * The schedule are expected to be written into nodes under this path. The schedules will have one line per
     * task, and on each line the partition the task should be assigned to. The partitions are numbered from [0,P), where
     * P stands for the number of partitions (number of workers requested by the topology).
     * <p/>
     * <p/>
     * Example: /stools/scheduler/schedules/topoXYZ
     */
    private static final String DEFAULT_SCHEDULING_SCHEDULES_ZK_PATH = "/stools/scheduler/schedule";

    /**
     * If a property with this name can be found in the storm configuration object the root path
     * in zookeeper will be set to the value of the provided property.
     */
    private static final String CONF_SCHEDULING_SCHEDULES_ZK_PATH = "scheduling.schedules.zk.path";

    private final static Logger LOG = LoggerFactory.getLogger(ZookeeperScheduler.class);

    /**
     * This default scheduler will be used if we cannot find any scheduling information.
     */
    private EvenScheduler evenScheduler;

    /**
     * This object is used for all communication with Zookeeper. We read the send graph from Zookeeper assuming
     * that { ch.uzh.ddis.stools.scheduler.SchedulingMetricsToZookeeperWriter} is registered and working.
     */
    private CuratorFramework zkClient;

    /**
     * The root path under which the schedules for each topology is expected to be stored.
     */
    private String zkRootPath;

    /**
     * We store all schedules for all topologies in this map, so we can decide if the schedule that we read from
     * Zookeeper is different from the one we have currently employed.
     */
    private Map<String, String> scheduleMap;

    /*
    topology name->task edge cut
     */
    private Map<String, Long> edgecutMap;

    private long last_schedule_time;

    private Jedis jedisClient;

    /**
     * This method computes and returns the path in Zookeeper under which the schedules are expected to be stored.
     * Because this path is configurable through the property {@value #CONF_SCHEDULING_SCHEDULES_ZK_PATH} the storm
     * configuration map must be passed as an argument.
     * <p/>
     * For each topology, there will be a sub node in this path, which in turn contains schedule as that znode's data.
     * For example schedule for topology test-topo-1-1394729310 would be stored in the znode:<br/>
     * <pre>
     * {rootPath}/test-topo-1-1394729310
     * </pre>
     *
     * @param stormConf the storm configuration map.
     * @return the path in Zookeeper under which the schedule should be stored.
     */
    private static String getConfiguredSchedulesZookeeperRootPath(Map stormConf) {
        String path;

        if (stormConf.containsKey(CONF_SCHEDULING_SCHEDULES_ZK_PATH)) {
            path = (String) stormConf.get(CONF_SCHEDULING_SCHEDULES_ZK_PATH);
        } else {
            path = DEFAULT_SCHEDULING_SCHEDULES_ZK_PATH;
        }

        return path;
    }

    @Override
    public void prepare(Map stormConf) {
        LOG.debug("Initializing the ZookeeperScheduler");

        this.scheduleMap = new HashMap<>();
        this.zkRootPath = getConfiguredSchedulesZookeeperRootPath(stormConf);
        this.evenScheduler = new EvenScheduler(); // default scheduler to use
        this.zkClient = Stootils.getConfiguredZkClient(stormConf); // open a connection to zookeeper
        this.last_schedule_time=0;
        this.jedisClient = RedisUtil.getInstance();
        this.edgecutMap = new HashMap<>();
        try {
            deletZkFolder("/stools");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void schedule(Topologies topologies, Cluster cluster) {
        if(topologies.getTopologies().isEmpty()){
            return;
        }
        long current_time=System.currentTimeMillis();

        if(current_time-last_schedule_time<60*1000){
            LOG.debug("It's not time to reschedule yet, " + (current_time-last_schedule_time)/1000 + " seconds have passed, other " + (120-(current_time-last_schedule_time)/1000));
            return;
        }
        Map<String, TopologyDetails> unscheduledTopologies; // key = topology-id

        LOG.debug("scheduling topologies...");
        unscheduledTopologies = new HashMap<>();

        /*
         This method will be called every couple of seconds (TODO: find out how often), so we don't need to attach
         a watcher. We just read the schedule from Zookeeper and check if it is different from the schedule we have
         currently in place. If the schedule is new, we re-schedule the topology.
         */

        for (TopologyDetails topology : topologies.getTopologies()) {


            String topologyId;
            String topologyName;
            String topologyPath;
            String schedule;

            topologyId = topology.getId();
            topologyName = topology.getName();
            topologyPath = this.zkRootPath + "/" + topologyName;
            LOG.info("Schedule topology "+topology.getName());

            //query redis for the task load
            Map<Integer, Long[]> taskLoadMap = new HashMap<>();

            //inter task traffic
            long[][] InterTaskTraffic;
            List<Long> edgeCut = new ArrayList<>();

            String cpu_load_map = RedisUtil.getTaskCPULoadMap(topologyId);
            String gpu_load_map = RedisUtil.getTaskGPULoadMap(topologyId);
            for (String key : jedisClient.hkeys(cpu_load_map)) {
                List<String> cpuLoad = jedisClient.hmget(cpu_load_map, key);
                if (cpuLoad == null || cpuLoad.get(0) == null) {
                    cpuLoad = new ArrayList<>();
                    cpuLoad.add("1");
                }
                List<String> gpuLoad = jedisClient.hmget(gpu_load_map, key);
                if (gpuLoad == null || gpuLoad.get(0) == null) {
                    LOG.debug("gpu load for task " + key + " not found, use default load");
                    gpuLoad = new ArrayList<>();
                    gpuLoad.add("1");
                }
                LOG.info("Task: " + key + " cpu load " + cpuLoad + " gpu load " + gpuLoad);
                taskLoadMap.put(Integer.parseInt(key), new Long[]{Long.parseLong(cpuLoad.get(0)), Long.parseLong(gpuLoad.get(0))});
            }
            LOG.info("Topology "+topologyName+" load metrics: "+taskLoadMap);
            /*
              phase 1 task -> task group
             */
            LOG.info("Phase 1 .............");
            try {
                LOG.info("Graph partition send graph of "+topologyName);

                InterTaskTraffic = taskPartition(taskLoadMap, topology, edgeCut);
                if(InterTaskTraffic==null){
                    LOG.info("Inter task traffic is not found");
                    unscheduledTopologies.put(topologyId, topology);
                    continue; // continue with next topology, maybe it will work when we are called next.
                }
                byte[] scheduleData;
                LOG.debug("Topo {}: Trying to read schedule from zookeeper at {}", topologyId, topologyPath);
                scheduleData = this.zkClient.getData().forPath(topologyPath);
                if (scheduleData == null) {
                    LOG.debug("Topo {}: The schedule on zookeeper was 'null'. Skipping topology.", topologyId);
                    unscheduledTopologies.put(topologyId, topology);
                    continue; // continue with next topology, maybe it will work when we are called next.
                }
                LOG.trace("Topo {}: Found schedule in zookeeper", topologyId);
                schedule = new String(scheduleData);
            } catch (KeeperException.NoNodeException e) {
                LOG.debug("Topo {}: Couldn't not find a schedule on zookeeper. Skipping topology.", topologyId);
                unscheduledTopologies.put(topologyId, topology);
                continue; // continue with next topology, maybe it will work when we are called next.
            } catch (Exception e) {
                LOG.warn("Error while reading schedule from zookeeper at " + topologyPath, e);
                unscheduledTopologies.put(topologyId, topology);
                continue; // continue with next topology, maybe it will work when we are called next.
            }

            // is the schedule we read from zk actually new?
            // current schedule map not contain the topologyid or schedule for this topo is different
            if (!this.scheduleMap.containsKey(topologyId) || !this.scheduleMap.get(topologyId).equals(schedule)) {
                if (!this.edgecutMap.containsKey(topologyId) || this.edgecutMap.get(topologyId) > edgeCut.get(0) * 1.1) {
                    SchedulerAssignment existingAssignment;
                    String[] taskToWorkerIndexString;
                    Map<Integer, Integer> taskToWorkerMap = new HashMap<>();
                    List<Set<ExecutorDetails>> executorsOfPartition; // the index is the partition/worker id
                    int numWorkers;
                    LOG.debug("Topo {}: Removing all assignments of topology", topologyId);

                    // the schedule is one number per line, each being the index of the partition the task for the given
                    // line should be assigned to. The line number is the task index (0-based!).
                    // the partition ids are 0-based because the METIS partitionings use that format
                    numWorkers = 0;
                    taskToWorkerIndexString = schedule.split("\n");
                    //put schedule string into an array
                    for (String aTaskToWorkerIndexString : taskToWorkerIndexString) {
                        String[] taskToWorker = aTaskToWorkerIndexString.split(" ");
                        int worker = Integer.parseInt(taskToWorker[1]); // worker = partition
                        int taskId = Integer.parseInt(taskToWorker[0]);
                        taskToWorkerMap.put(taskId, worker);
                        numWorkers = Math.max(numWorkers, worker + 1); // partitions are 0-based
                    }
                    LOG.info("Topo {}: Schedule is considering {} tasks that have to be scheduled to {} workers",
                            topologyId, taskToWorkerMap.size(), numWorkers);
                    // initialize the assignment collections
                    executorsOfPartition = new ArrayList<>();
                    for (int i = 0; i < numWorkers; i++) {
                        executorsOfPartition.add(new HashSet<ExecutorDetails>());
                    }
                    for (ExecutorDetails executor : topology.getExecutors()) {
                        int taskId;
                        //there must be exactly one task in an executor
                        if (executor.getStartTask() != executor.getEndTask()) {
                            String errorMsg = String.format("Cannot handle executors with more than one task. Found" +
                                            " an executor with starTask=%d and endTask=%d for component %s.",
                                    executor.getStartTask(), executor.getEndTask(),
                                    topology.getExecutorToComponent().get(executor));
                            throw new IllegalArgumentException(errorMsg);
                        }
                        // taskIds are 1-based
                        // if you get exception around here for taskIds not matching, maybe task ids are not always 1-based ;-)
                        taskId = executor.getStartTask();
                        if (taskId < 1) {
                            LOG.info("Topo {}: Assigning special task with id {} of component {} to partition 0",
                                    topologyId,
                                    taskId,
                                    topology.getExecutorToComponent().get(executor));
                        }
                        //assign task to worker slots according to schedule
                        else if (taskToWorkerMap.containsKey(taskId)) {
                            int partitionId;
                            partitionId = taskToWorkerMap.get(taskId);
                            LOG.info("Topo {}: assigning task with id {} to partition {}",
                                    topologyId, taskId, partitionId);
                            executorsOfPartition.get(partitionId).add(executor);
                        } else { // for all ackers and metrics, we do round-robbing
                            int partitionId;
                            partitionId = taskId % numWorkers;
                            LOG.info("Topo {}: assigning task with id {} to partition {} (round-robin)",
                                    topologyId, taskId, partitionId);
                            executorsOfPartition.get(partitionId).add(executor);
                            taskToWorkerMap.put(taskId, partitionId);
                        }
                    }
                    LOG.info("Task to worker map: " + taskToWorkerMap);
                    LOG.info("Phase 1 completed............");
                    LOG.info("Phase 2 .....................");
                /*
                  phase2 worker -> supervisor partition
                 */
                    // free all currently used slots
                    existingAssignment = cluster.getAssignmentById(topologyId);
                    if (existingAssignment != null) {
                        LOG.debug("Topo {}: Freeing all currently used slots", topologyId);
                        for (WorkerSlot slot : existingAssignment.getSlots()) {
                            cluster.freeSlot(slot);
                        }
                    }

                    List<SupervisorSlot> availableSupervisors = new LinkedList<>();
                    int totalavailableSlots = 0;
                    for (SupervisorDetails supervisor : cluster.getSupervisors().values()) {
                        int availablePorts = cluster.getAssignablePorts(supervisor).size();
                        totalavailableSlots += availablePorts;
                        if (availablePorts > 0 && !cluster.isBlackListed(supervisor.getId())) {
                            availableSupervisors.add(new SupervisorSlot(supervisor, cluster.getAssignableSlots(supervisor)));
                        }
                    }
                    LOG.info("Available supervisors: " + availableSupervisors);
                    if (totalavailableSlots < numWorkers) {
                        LOG.error("There is not enough worker to assign " + topologyName);
                        continue;
                    }
                    LOG.info("Total slots is " + totalavailableSlots);

                    List<SupervisorSlot> choosedSupervisors = new LinkedList<>();

                    for (int i = 0; i < numWorkers; i++) {
                        Collections.sort(availableSupervisors);
                        LOG.info("Sort available supervisor: " + availableSupervisors);
                        SupervisorSlot ss = availableSupervisors.get(availableSupervisors.size() - 1);
                        WorkerSlot ws = ss.avaiableSlots.remove(0);
                        ss.choosedSlots.add(ws);
                        LOG.info("Choose supervisor: " + ss + " worker: " + ws.getPort());
                        if (!choosedSupervisors.contains(ss))
                            choosedSupervisors.add(ss);
                    }
                    LOG.info("Choosed supervisors: " + choosedSupervisors);
                    List<Integer> partitionSize = new LinkedList<>();
                    for (SupervisorSlot ss : choosedSupervisors)
                        partitionSize.add(ss.choosedSlots.size());
                    try {
                        LOG.info("Call worker partition method");
                        List<List<Integer>> supervisorToWorkers = workerPartition(topologyName, InterTaskTraffic, taskToWorkerMap, numWorkers, partitionSize);
                        for (SupervisorSlot ss : choosedSupervisors) {
                            if (supervisorToWorkers != null && supervisorToWorkers.size() > 0) {
                                List<Integer> workers = supervisorToWorkers.remove(0);
                                for (WorkerSlot ws : ss.choosedSlots)
                                    cluster.assign(ws, topologyId, executorsOfPartition.get(workers.remove(0)));
                            } else {
                                LOG.error("Supervisor to worker list is not enough");
                                return;
                            }
                        }
                        LOG.info("Phase 2 completed............");
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    // topology was successfully scheduled, remember the schedule we used for this
                    this.scheduleMap.put(topologyId, schedule);
                    if(edgeCut.size()>0)
                        this.edgecutMap.put(topologyId, edgeCut.get(0));

                } else {
                    LOG.info("The schedule edge cut is not good enough. Not doing anything");
                }
            }else {
                LOG.info("The schedule found in Zookeeper has already been applied. Not doing anything.");
            }
        }
        if (unscheduledTopologies.size() > 0) {
            LOG.debug("Scheduling {} remaining topologies using the even scheduler ...", unscheduledTopologies.size());
            this.evenScheduler.schedule(new Topologies(unscheduledTopologies), cluster);
        }
        last_schedule_time=current_time;
    }

    private List<List<Integer>> workerPartition(String topologyName, long[][] InterTaskTraffic, Map<Integer, Integer> taskOfPartition, int numWorkers, List<Integer> partitionSize) throws IOException {
        List<List<Integer>> ret = new ArrayList<>();

        LOG.info("Inter task traffic: "+Arrays.deepToString(InterTaskTraffic));
        LOG.info("Task to partition: "+taskOfPartition);
        LOG.info("Partition size: "+partitionSize);

        if(partitionSize.size()==0) {
            LOG.error("Phase 2 : partition number must >0");
            return null;
        }
        if(partitionSize.size()==1){
            LOG.info("Phase 2 : partition number =1 put all worker into 1 partition");
            List<Integer> workers = new ArrayList<>();
            for(int i=0;i<numWorkers;i++)
                workers.add(i);
            ret.add(workers);
            return ret;
        }
        //initial return list
        for(int i=0;i<partitionSize.size();i++){
            ret.add(new ArrayList<Integer>());
        }

        //calculate inter worker traffic
        long[][] InterWorkerTraffic = new long[numWorkers][numWorkers];
        int taskNum = InterTaskTraffic.length;
        for(int fromTaskId=1;fromTaskId<taskNum;fromTaskId++){
            int fromWorkerId = taskOfPartition.get(fromTaskId);
            for(int toTaskId=fromTaskId;toTaskId<taskNum;toTaskId++){
                int toWorkerId = taskOfPartition.get(toTaskId);
                if(fromWorkerId==toWorkerId)
                    continue;
                InterWorkerTraffic[fromWorkerId][toWorkerId] += InterTaskTraffic[fromTaskId][toTaskId];
                InterWorkerTraffic[toWorkerId][fromWorkerId] += InterTaskTraffic[fromTaskId][toTaskId];
            }
        }
        LOG.info("Inter worker traffic: "+Arrays.deepToString(InterWorkerTraffic));

        String graph_file_parent="/tmp/graph_file/worker/";
        new File(graph_file_parent).mkdirs();
        String weight_file =graph_file_parent+topologyName+"_weight";

        //write weight_file
        BufferedWriter weight_writer=new BufferedWriter(new FileWriter(weight_file));
        for(int i=0;i<partitionSize.size();i++){
            weight_writer.write(i+" = "+(float)partitionSize.get(i)/numWorkers+"\n");
        }
        weight_writer.close();

        //write graph file
        String graph_file=graph_file_parent+topologyName+".graph";
        BufferedWriter writer=new BufferedWriter(new FileWriter(graph_file));
        List<Map<Integer,Long>> trafficList = new ArrayList<>();
        int edgeNumber = 0;
        for(int i=0;i<InterWorkerTraffic.length;i++){
            Map<Integer, Long> trafficMap=new HashMap<>();
            for(int j=0;j<InterWorkerTraffic[0].length;j++)
                if(InterWorkerTraffic[i][j]>0) {
                    trafficMap.put(j, InterWorkerTraffic[i][j]);
                    edgeNumber++;
                }
            trafficList.add(trafficMap);
        }
        LOG.info("Traffic list: "+trafficList);
        //write header of graph
        writer.write(numWorkers+" "+edgeNumber/2+" "+"011"+"\n");
        for(int i=0;i<numWorkers;i++){
            String line = "1";
            for(Integer toWorker : trafficList.get(i).keySet())
                line+=" "+(toWorker+1)+" "+trafficList.get(i).get(toWorker);
            line+="\n";
            writer.write(line);
        }
        writer.close();
        Runtime runtime=Runtime.getRuntime();
        Process proc = runtime.exec(new String[]{Stootils.metis_binary, graph_file, Integer.toString(partitionSize.size()), "-tpwgts="+weight_file, "-ptype=rb"});
        BufferedReader METIS_reader = new BufferedReader(new InputStreamReader(proc.getInputStream()));
        String message;
        while((message=METIS_reader.readLine())!=null)
            LOG.debug(message);
        String partition_file=graph_file+".part."+partitionSize.size();
        if(!new File(partition_file).exists()) {
            LOG.error("Partition file not found");
        }
        BufferedReader br=new BufferedReader(new FileReader(partition_file));
        String line;
        int lineNum=0;
        while((line=br.readLine())!=null) {
            int supervisorId = Integer.parseInt(line);
            ret.get(supervisorId).add(lineNum);
            lineNum++;
        }
        LOG.info("Phase 2: partition result "+ret.toString());
        return ret;
    }
    private long[][] taskPartition(Map<Integer, Long[]> taskLoadMap, TopologyDetails topologyDetails, List<Long> edgeCut) {

        String topologyName = topologyDetails.getName();
        String sendgraph_zk_path = Stootils.metrics_zk_path + "/" + topologyName + "/sendgraph";
        String topologySchedulePath = Stootils.schedule_zk_path + "/" + topologyName;
        String topologyWorkerPath = Stootils.metrics_zk_path + "/" + topologyName + "/config/topology.workers";
        long[][] InterTaskTraffic = null;
        int taskNum_all = topologyDetails.getExecutors().size();
        List<Integer> taskWithTrafficIds = new ArrayList<>();
        Map<Integer, Integer> vetexIdtoTaskId = new HashMap<>();
        Map<Integer, Integer> taskIdtoVetexId = new HashMap<>();
        try {
            LOG.info("Read send graph of topology: " + topologyName + " from path: " + sendgraph_zk_path);
            if (zkClient.checkExists().forPath(sendgraph_zk_path) == null) {
                LOG.error("There is no send graph found in given zk path");
                return null;
            }
            byte[] byte_send_graph = zkClient.getData().forPath(sendgraph_zk_path);
            String send_graph = new String(byte_send_graph);
            LOG.info("Send graph of topology " + topologyName + " is: " + send_graph);
            Gson gson = new Gson();
            Map<String, Object> o = gson.fromJson(send_graph, Map.class);
            List<Map<String, Object>> links = (List<Map<String, Object>>) o.get("links");
            //get and sort all task with metrics
            for (Map link : links) {
                int sourceId = ((Double) link.get("source")).intValue();
                int targetId = ((Double) link.get("target")).intValue();
                if(!taskWithTrafficIds.contains(sourceId))
                    taskWithTrafficIds.add(sourceId);
                if(!taskWithTrafficIds.contains(targetId))
                    taskWithTrafficIds.add(targetId);
            }
            Collections.sort(taskWithTrafficIds);
            LOG.info(taskWithTrafficIds.toString());
            int taskWithTrafficNum = taskWithTrafficIds.size();
            //METIS vertex id is 1-based
            for (int i = 0; i < taskWithTrafficNum; i++) {
                vetexIdtoTaskId.put(i + 1, taskWithTrafficIds.get(i));
                taskIdtoVetexId.put(taskWithTrafficIds.get(i), i + 1);
            }
            LOG.debug("Vetex ID to task ID: " + vetexIdtoTaskId);
            LOG.debug("Task ID to vetex ID: " + taskIdtoVetexId);
            InterTaskTraffic = new long[taskNum_all + 1][taskNum_all + 1];
            String graph_file_parent = "/tmp/graph_file/task/";
            new File(graph_file_parent).mkdirs();
            String graph_file = "/tmp/graph_file/task/" + topologyName + ".graph";
            BufferedWriter writer = new BufferedWriter(new FileWriter(graph_file));
            //write header of graph
            writer.write(taskWithTrafficNum + " " + links.size() + " " + "011" + " " + "2\n");
            Map<Integer, String> metisInput = new HashMap<>();
            for (int i = 1; i <= taskWithTrafficNum; i++) {
                metisInput.put(i, "");
            }
            for (Map link : links) {
                int sourceId = ((Double) link.get("source")).intValue();
                int targetId = ((Double) link.get("target")).intValue();
                int metisSourceId = taskIdtoVetexId.get(sourceId);
                int metisTargetId = taskIdtoVetexId.get(targetId);
                long traffic = ((Double) link.get("value")).longValue();
                InterTaskTraffic[sourceId][targetId] = traffic;
                InterTaskTraffic[targetId][sourceId] = traffic;
                String line = metisInput.get(metisSourceId) + " " + metisTargetId + " " + traffic;
                metisInput.put(metisSourceId, line);
                line = metisInput.get(metisTargetId) + " " + metisSourceId + " " + traffic;
                metisInput.put(metisTargetId, line);
            }
            for (int tid : taskIdtoVetexId.keySet()) {
                int vetexId = taskIdtoVetexId.get(tid);
                Long[] taskLoad = taskLoadMap.get(tid);
                if (taskLoad == null || taskLoad.length != 2) {
                    LOG.warn("Task Load of " + tid + " is not correct, use default value");
                    taskLoad = new Long[]{1L, 1L};
                }
                metisInput.put(vetexId, taskLoad[0] + " " + taskLoad[1] + " " + metisInput.get(vetexId) + "\n");
            }
            LOG.info("---------- Graph file -------------");
            for (int i = 1; i <= taskWithTrafficNum; i++) {
                writer.write(metisInput.get(i));
                LOG.info(metisInput.get(i));
            }
            LOG.info("-----------------------------------");

            writer.close();
            LOG.info("Graph file write ok");
            if (zkClient.checkExists().forPath(topologyWorkerPath) == null) {
                LOG.info("There is no worker num found in given zk path, can't call metis partition");
                return null;
            }
            byte[] bytes = zkClient.getData().forPath(topologyWorkerPath);
            int num_worker = 0;
            int bit = 0;
            for (int i = 7; i >= 0; i--) {
                int tmp = bytes[i] << bit;
                bit += 8;
                num_worker += tmp;
            }
            if (num_worker <= 1) {
                throw new RuntimeException("Worker number must be >1");
            }
            LOG.debug("worker num of topology " + topologyName + " is " + num_worker);
            LOG.info("Calling METIS to generate partition");
            Runtime runtime = Runtime.getRuntime();
            Process proc = runtime.exec(new String[]{Stootils.metis_binary, graph_file, Integer.toString(num_worker), "-ufactor=300"});

            BufferedReader METIS_reader = new BufferedReader(new InputStreamReader(proc.getInputStream()));
            String message;
            while ((message = METIS_reader.readLine()) != null) {
                if (message.contains("Edgecut")) {
                    LOG.info(message);
                    String[] splitMessage = message.split(",")[0].split(" ");
                    edgeCut.clear();
                    edgeCut.add(Long.parseLong(splitMessage[splitMessage.length-1]));
                }
            }
            String line;
            String partition_file = graph_file + ".part." + num_worker;
            if (!new File(partition_file).exists()) {
                LOG.error("Partition file not found");
            }
            BufferedReader br = new BufferedReader(new FileReader(partition_file));
            String schedule = "";
            int lineNum = 1;
            while ((line = br.readLine()) != null) {
                int taskId = vetexIdtoTaskId.get(lineNum);
                schedule += taskId + " " + line + "\n";
                lineNum++;
            }
            LOG.info("Phase 1 METIS schedule result " + Arrays.toString(schedule.split("\n")));
            if (schedule.length() > 0) {
                if (zkClient.checkExists().forPath(topologySchedulePath) == null) {
                    zkClient.create().creatingParentsIfNeeded().forPath(topologySchedulePath, schedule.getBytes());
                } else {
                    zkClient.setData().forPath(topologySchedulePath, schedule.getBytes());
                }
            } else
                LOG.warn("Don't have partition results");

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return InterTaskTraffic;
    }

    private void deletZkFolder(String path) throws Exception {
        List<String> sons;
        if(zkClient.checkExists().forPath(path)==null)
            return;
        try {
            sons = zkClient.getChildren().forPath(path);
            if(!sons.isEmpty()) for (String son : sons) {
                String sonPath = path + "/" + son;
                deletZkFolder(sonPath);
            }
            zkClient.delete().forPath(path);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
