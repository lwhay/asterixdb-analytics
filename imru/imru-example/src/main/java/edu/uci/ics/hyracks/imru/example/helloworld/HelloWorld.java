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

package edu.uci.ics.hyracks.imru.example.helloworld;

import edu.uci.ics.hyracks.imru.example.utils.Client;

/**
 * This example demonstrate how data flows through IMRU.
 * The input is six files. Each file has one character.
 * The map operator pass each file's content to reduce operator.
 * The reduce operator pass the combined content to update operator.
 * The final model is the combined content annotated with each
 * operator.
 */
public class HelloWorld {
    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            // if no argument is given, the following code
            // creates default arguments to run the example
            String cmdline = "";
            int totalNodes = 8;
            boolean useExistingCluster = Client.isServerAvailable(Client.getLocalIp(), 3099);
            if (useExistingCluster) {
                // hostname of cluster controller
                String ip = Client.getLocalIp();
                cmdline += "-host " + ip + " -port 3099";
                System.out.println("Connecting to " + Client.getLocalIp());
            } else {
                // debugging mode, everything run in one process
                cmdline += "-host localhost -port 3099 -debug -disable-logging";
                cmdline += " -debugNodes " + totalNodes;
                cmdline += " -agg-tree-type nary -fan-in 4";
                cmdline += " -agg-tree-type none";

                System.out.println("Starting hyracks cluster");
            }

            String exampleData = System.getProperty("user.home") + "/hyracks/imru/imru-example/data/helloworld";
            int n = 6;
            if (useExistingCluster) {
                cmdline += " -example-paths " + exampleData + "/hello0.txt";
                for (int i = 1; i < n; i++)
                    cmdline += "," + exampleData + "/hello" + i + ".txt";
            } else {
                cmdline += " -example-paths NC0:" + exampleData + "/hello0.txt";
                for (int i = 1; i < n; i++)
                    cmdline += ",NC" + (i % totalNodes) + ":" + exampleData + "/hello" + i + ".txt";
            }
            System.out.println("Using command line: " + cmdline);
            args = cmdline.split(" ");
        }

        try {
            String finalModel = Client.run(new HelloWorldJob(), "", args);
            System.out.println("FinalModel: " + finalModel);
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(0);
        }
        System.exit(0);
    }
}
