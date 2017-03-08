/**
 *  @author Lorenz Fischer
 *
 *  Copyright 2016 University of Zurich
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package edu.fudan.storm.utils;


import org.apache.storm.Config;
import org.apache.storm.shade.org.apache.curator.framework.CuratorFramework;
import org.apache.storm.utils.ZookeeperAuthInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 */
public class Stootils {

    public static final String current_user = System.getProperty("user.name");
    public static final String metrics_zk_path = "/stools/scheduler/metrics"   ;   // where we expect the metrics to be written to
    public static final String schedule_zk_path  = "/stools/scheduler/schedule" ;  // where we will write the schedules to
    public static final String metis_binary = "/home/"+current_user+"/lib/metis/bin/gpmetis";
    private final static Logger LOG = LoggerFactory.getLogger(Stootils.class);
    private static CuratorFramework zkClientSingleton;

    /**
     * Creates or returns the curator zookeeper client object for the given storm configuration object. According to
     * http://curator.apache.org/curator-framework instances of CuratorFramework are fully thread-safe and should be shared
     * within an application per zk-cluster. We assume that there is only one version of the storm configuration object
     * and return a singleton instance of the zkClient.
     *
     * @param stormConf the storm configuration object, which will be used to create the CuratorFramework instance in
     *                  the case that the singleton instance is null.
     * @return a singleton instance created from the first call of this method.
     */
    @SuppressWarnings("unchecked") // the list of zookeeper servers is a list, otherwise we have bigger problems
    public static synchronized CuratorFramework getConfiguredZkClient(Map stormConf) {
        if (zkClientSingleton == null) {
            LOG.info("Creating CuratorFramework client for ZK server at {}:{}", stormConf.get(Config.STORM_ZOOKEEPER_SERVERS), stormConf.get(Config.STORM_ZOOKEEPER_PORT));
            zkClientSingleton = org.apache.storm.utils.Utils.newCurator(stormConf,
                    (List<String>) stormConf.get(Config.STORM_ZOOKEEPER_SERVERS),
                    stormConf.get(Config.STORM_ZOOKEEPER_PORT),
                    "/",
                    new ZookeeperAuthInfo(stormConf));
            zkClientSingleton.start();
        }
        return zkClientSingleton;
    }
    public static String collectionToString(Collection<?> list) {
        if (list == null)
            return "null";
        if (list.isEmpty())
            return "<empty list>";
        StringBuffer sb = new StringBuffer();
        int i = 0;
        for (Object item : list) {
            sb.append(item.toString());
            if (i < list.size() - 1)
                sb.append(", ");
            i++;
        }
        return sb.toString();
    }
}
