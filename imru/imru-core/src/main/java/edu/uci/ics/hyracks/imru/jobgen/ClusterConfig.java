/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.imru.jobgen;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hyracks.api.client.HyracksConnection;
import org.apache.hyracks.api.client.NodeControllerInfo;
import org.apache.hyracks.api.constraints.PartitionConstraintHelper;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.JobSpecification;

import edu.uci.ics.hyracks.imru.dataflow.IMRUOperatorDescriptor;
import edu.uci.ics.hyracks.imru.file.IMRUFileSplit;

public class ClusterConfig {

    private static Logger LOG = Logger.getLogger(ClusterConfig.class.getName());

    private static String[] NCs;
    private static String confPath = "conf/cluster";
    private static Map<String, List<String>> ipToNcMapping;

    /**
     * let tests set config path to be whatever
     * 
     * @param confPath
     */
    public static void setConfPath(String confPath) {
        ClusterConfig.confPath = confPath;
    }

    /**
     * get NC names running on one IP address
     * 
     * @param ipAddress
     * @return
     * @throws HyracksDataException
     */
    public static List<String> getNCNames(String ipAddress) throws HyracksException {
        if (NCs == null) {
            try {
                loadClusterConfig();
            } catch (IOException e) {
                throw new HyracksException(e);
            }
        }
        return ipToNcMapping.get(ipAddress);
    }

    /**
     * Set location constraints for an operator based on the locations of input
     * files in HDFS. Randomly assigns partitions to NCs where the HDFS files
     * are local; assigns the rest randomly.
     * 
     * @param spec
     *            A job specification.
     * @param operator
     *            The operator that will be constrained.
     * @param splits
     *            A list of InputSplits specifying files in HDFS.
     * @param random
     *            A source of randomness (so the partition-assignment can be
     *            repeated across iterations, provided that the HDFS file
     *            locations don't change).
     * @return The assigned partition locations.
     * @throws IOException
     * @throws HyracksException
     */
    public static String[] setLocationConstraint(JobSpecification spec, IMRUOperatorDescriptor operator,
            InputSplit[] hdfsSplits, IMRUFileSplit[] splits, Random random) throws IOException {
        if (NCs == null)
            loadClusterConfig();
        if (splits.length == 0)
            return new String[0];

        if (hdfsSplits == null) {
            int partitionCount = splits.length;
            String[] partitionLocations = new String[partitionCount];
            for (int partition = 0; partition < partitionCount; partition++) {
                int pos = partition % NCs.length;
                String path = splits[partition].getPath();
                int t = path.indexOf(":");
                if (t > 0)
                    partitionLocations[partition] = path.substring(0, t);
                else
                    partitionLocations[partition] = NCs[pos];
            }
            if (operator != null) {
                PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, operator, partitionLocations);
                PartitionConstraintHelper.addPartitionCountConstraint(spec, operator, partitionCount);
            }
            return partitionLocations;
        }
        int partitionCount = splits.length;
        String[] partitionLocations = new String[partitionCount];
        int localAssignments = 0;
        int nonlocalAssignments = 0;
        for (int partition = 0; partition < partitionCount; partition++) {
            String[] localHosts = hdfsSplits[partition].getLocations();
            // Remove nondeterminism from the call to getLocations():
            Collections.sort(Arrays.asList(localHosts));
            Collections.shuffle(Arrays.asList(localHosts), random);
            if (localHosts.length > 0) {
                LOG.info("Partition " + partition + " is local at " + localHosts.length + " hosts: "
                        + StringUtils.join(localHosts, ", "));
                for (int host = 0; host < localHosts.length; host++) {
                    InetAddress[] hostIps = InetAddress.getAllByName(localHosts[host]);
                    for (InetAddress ip : hostIps) {
                        if (ipToNcMapping.get(ip.getHostAddress()) != null) {
                            List<String> ncs = ipToNcMapping.get(ip.getHostAddress());
                            int pos = random.nextInt(ncs.size());
                            partitionLocations[partition] = ncs.get(pos);
                            LOG.info("Partition " + partition + " assigned to " + ncs.get(pos) + ", where it is local.");
                            localAssignments++;
                            break;
                        }
                    }
                    if (partitionLocations[partition] != null) {
                        break;
                    }
                }
                if (partitionLocations[partition] == null) {
                    int pos = random.nextInt(NCs.length);
                    partitionLocations[partition] = NCs[pos];
                    nonlocalAssignments++;
                    LOG.info("Partition " + partition + " assigned to " + NCs[pos]
                            + " because there is no NC where it is local.");
                }
            } else {
                int pos = random.nextInt(NCs.length);
                partitionLocations[partition] = NCs[pos];
                nonlocalAssignments++;
                LOG.info("Partition " + partition + " assigned to " + NCs[pos]
                        + " becasue getLocations() returned no locations.");

            }
        }
        if (LOG.isLoggable(Level.INFO)) {
            LOG.info("NC partition counts:");
            Map<String, MutableInt> ncPartitionCounts = new HashMap<String, MutableInt>();
            for (int i = 0; i < partitionLocations.length; i++) {
                if (ncPartitionCounts.get(partitionLocations[i]) == null) {
                    ncPartitionCounts.put(partitionLocations[i], new MutableInt(1));
                } else {
                    ncPartitionCounts.get(partitionLocations[i]).increment();
                }
            }
            for (Map.Entry<String, MutableInt> entry : ncPartitionCounts.entrySet()) {
                LOG.info(entry.getKey() + ": " + entry.getValue().intValue() + " partitions");
            }
        }
        double localityPercentage = ((1.0 * localAssignments) / (localAssignments + nonlocalAssignments)) * 100;
        if (operator != null) {
            LOG.info(operator.getClass().getSimpleName() + ": " + localAssignments + " local; " + nonlocalAssignments
                    + " non-local; " + localityPercentage + "% locality");
            PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, operator, partitionLocations);
            PartitionConstraintHelper.addPartitionCountConstraint(spec, operator, partitionCount);
        }
        return partitionLocations;
    }

    public static void setConf(HyracksConnection hcc) throws Exception {
        Map<String, NodeControllerInfo> map = hcc.getNodeControllerInfos();
        List<String> ncNames = new ArrayList<String>();
        ipToNcMapping = new HashMap<String, List<String>>();
        for (String key : map.keySet()) {
            NodeControllerInfo info = map.get(key);
            String id = info.getNodeId();
            byte[] ip = info.getNetworkAddress().lookupIpAddress();
            StringBuilder sb = new StringBuilder();
            for (byte b : ip) {
                if (sb.length() > 0)
                    sb.append(".");
                sb.append(b & 0xFF);
            }
            //            LOG.info(id + " " + sb);
            ncNames.add(id);
            List<String> ncs = ipToNcMapping.get(id);
            if (ncs == null) {
                ncs = new ArrayList<String>();
                ipToNcMapping.put(id, ncs);
            }
            ncs.add(sb.toString());
        }
        NCs = ncNames.toArray(new String[ncNames.size()]);
    }

    private static void loadClusterConfig() throws IOException {
        String line = "";
        ipToNcMapping = new HashMap<String, List<String>>();
        if (!new File(confPath).exists()) {
            if (NCs.length > 0)
                return;
            throw new HyracksException("Can't find " + confPath);
            // NCs=new String[0];
            // return;
        }
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(confPath)));
        List<String> ncNames = new ArrayList<String>();
        while ((line = reader.readLine()) != null) {
            String[] ncConfig = line.split(" ");
            ncNames.add(ncConfig[1]);

            List<String> ncs = ipToNcMapping.get(ncConfig[0]);
            if (ncs == null) {
                ncs = new ArrayList<String>();
                ipToNcMapping.put(ncConfig[0], ncs);
            }
            ncs.add(ncConfig[1]);
        }
        reader.close();
        NCs = ncNames.toArray(new String[0]);
    }
}
