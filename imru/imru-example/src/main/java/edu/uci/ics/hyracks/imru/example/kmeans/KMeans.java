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

package edu.uci.ics.hyracks.imru.example.kmeans;

import edu.uci.ics.hyracks.imru.example.utils.Client;

/**
 * Start a local cluster within the process and run the kmeans example.
 */
public class KMeans {
    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            // if no argument is given, the following code
            // creates default arguments to run the example
            String cmdline = "";
            if (Client.isServerAvailable(Client.getLocalIp(), 3099)) {
                // hostname of cluster controller
                cmdline += "-host " + Client.getLocalIp() + " -port 3099";
                System.out.println("Connecting to " + Client.getLocalIp());
            } else {
                // debugging mode, everything run in one process
                cmdline += "-host localhost -port 3099 -debug -disable-logging";
                System.out.println("Starting hyracks cluster");
            }

            boolean useHDFS = false;
            if (useHDFS) {
                // hadoop config path
                cmdline += " -hadoop-conf " + System.getProperty("user.home") + "/hadoop-0.20.2/conf";
                cmdline += " -example-paths /kmeans/kmeans0.txt,/kmeans/kmeans1.txt";
            } else {
                String exampleData = System.getProperty("user.home") + "/hyracks/imru/imru-example/data/kmeans";
                cmdline += " -example-paths " + exampleData + "/kmeans0.txt," + exampleData + "/kmeans1.txt";
            }
            System.out.println("Using command line: " + cmdline);
            args = cmdline.split(" ");
        }

        int k = 3;

        double minDis = Double.MAX_VALUE;
        KMeansModel bestModel = null;
        for (int modelId = 0; modelId < 3; modelId++) {
            System.out.println("trial " + modelId);
            KMeansModel initModel = Client.run(new RandomSelectJob(k), new KMeansModel(k, 1), args);
            System.out.println("InitModel: " + initModel);

            initModel.roundsRemaining = 20;

            KMeansModel finalModel = Client.run(new KMeansJob(k), initModel, args);
            System.out.println("FinalModel: " + finalModel);
            System.out.println("DistanceSum: " + finalModel.lastDistanceSum);
            if (finalModel.lastDistanceSum < minDis) {
                minDis = finalModel.lastDistanceSum;
                bestModel = finalModel;
            }
        }
        System.out.println("BestModel: " + bestModel);
        System.exit(0);
    }
}
