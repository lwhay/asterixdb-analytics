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

package edu.uci.ics.hyracks.imru.runtime;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.EnumSet;
import java.util.UUID;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hyracks.api.client.HyracksConnection;
import org.apache.hyracks.api.deployment.DeploymentId;
import org.apache.hyracks.api.job.JobFlag;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.api.job.JobStatus;

import edu.uci.ics.hyracks.imru.api.IIMRUDataGenerator;
import edu.uci.ics.hyracks.imru.api.IIMRUJob2;
import edu.uci.ics.hyracks.imru.jobgen.IMRUJobFactory;
import edu.uci.ics.hyracks.imru.runtime.bootstrap.IMRUConnection;

/**
 * Schedules iterative map reduce update jobs.
 * 
 * @param <Model>
 *            The class used to represent the global model that is
 *            persisted between iterations.
 * @author Josh Rosen
 */
public class IMRUDriver<Model extends Serializable, Data extends Serializable> {
    private final static Logger LOGGER = Logger.getLogger(IMRUDriver.class.getName());
    private final IIMRUJob2<Model, Data> imruSpec;
    private Model model;
    private final HyracksConnection hcc;
    DeploymentId deploymentId;
    private final IMRUConnection imruConnection;
    private final IMRUJobFactory jobFactory;
    private final Configuration conf;
    private final String app;
    private final UUID id;

    private int iterationCount;
    public boolean memCache = false;
    public boolean noDiskCache = false;
    public int frameSize;
    public String modelFileName;
    public String localIntermediateModelPath;

    /**
     * Construct a new IMRUDriver.
     * 
     * @param hcc
     *            A client connection to the cluster controller.
     * @param imruSpec
     *            The IIMRUJob2 to use.
     * @param initialModel
     *            The initial global model.
     * @param jobFactory
     *            A factory for constructing Hyracks dataflows for
     *            this IMRU job.
     * @param conf
     *            A Hadoop configuration used for HDFS settings.
     * @param tempPath
     *            The DFS directory to write temporary files to.
     * @param app
     *            The application name to use when running the jobs.
     */
    public IMRUDriver(HyracksConnection hcc, DeploymentId deploymentId, IMRUConnection imruConnection,
            IIMRUJob2<Model, Data> imruSpec, Model initialModel, IMRUJobFactory jobFactory, Configuration conf,
            String app) {
        this.imruSpec = imruSpec;
        this.model = initialModel;
        this.hcc = hcc;
        this.deploymentId = deploymentId;
        this.imruConnection = imruConnection;
        this.jobFactory = jobFactory;
        this.conf = conf;
        this.app = app;
        id = jobFactory.getId();
        iterationCount = 0;
    }

    public String getModelName() {
        String s;
        if (modelFileName != null)
            s = modelFileName;
        else
            s = "IMRU-" + id;
        s = s.replaceAll(Pattern.quote("${NODE_ID}"), "CC");
        return s;
    }

    /**
     * Run iterative map reduce update.
     * 
     * @return The JobStatus of the IMRU computation.
     * @throws Exception
     */
    public JobStatus run() throws Exception {
        LOGGER.info("Starting IMRU job with driver id " + id);
        iterationCount = 0;
        // The path containing the model to be used as input for a
        // round.
        // The path containing the updated model written by the
        // Update operator.

        // For the first round, the initial model is written by the
        // driver.
        if (localIntermediateModelPath != null)
            writeModelToFile(model, new File(localIntermediateModelPath, getModelName() + "-iter" + 0));

        imruConnection.uploadModel(this.getModelName(), model);

        // Data load
        if (!noDiskCache || jobFactory.confFactory.useHDFS()) {
            LOGGER.info("Starting data load");
            long loadStart = System.currentTimeMillis();
            JobStatus status = runDataLoad();
            long loadEnd = System.currentTimeMillis();
            LOGGER.info("Finished data load in " + (loadEnd - loadStart) + " milliseconds");
            if (status == JobStatus.FAILURE) {
                LOGGER.severe("Failed during data load");
                return JobStatus.FAILURE;
            }
        }

        // Iterations
        do {
            iterationCount++;

            LOGGER.info("Starting round " + iterationCount);
            long start = System.currentTimeMillis();
            JobStatus status = runIMRUIteration(getModelName(), iterationCount);
            long end = System.currentTimeMillis();
            LOGGER.info("Finished round " + iterationCount + " in " + (end - start) + " milliseconds");

            if (status == JobStatus.FAILURE) {
                LOGGER.severe("Failed during iteration " + iterationCount);
                return JobStatus.FAILURE;
            }
            model = (Model) imruConnection.downloadModel(this.getModelName());
            if (model == null)
                throw new Exception("Can't download model");
            if (localIntermediateModelPath != null)
                writeModelToFile(model, new File(localIntermediateModelPath, getModelName() + "-iter" + iterationCount));

            // TODO: clean up temporary files
        } while (!imruSpec.shouldTerminate(model));
        return JobStatus.TERMINATED;
    }

    /**
     * @return The number of iterations performed.
     */
    public int getIterationCount() {
        return iterationCount;
    }

    /**
     * @return The most recent global model.
     */
    public Model getModel() {
        return model;
    }

    /**
     * @return The IMRU job ID.
     */
    public UUID getId() {
        return id;
    }

    /**
     * Run the dataflow to cache the input records.
     * 
     * @return The JobStatus of the job after completion or failure.
     * @throws Exception
     */
    private JobStatus runDataLoad() throws Exception {
        JobSpecification job = jobFactory.generateDataLoadJob(imruSpec, memCache);
        //                byte[] bs=JavaSerializationUtils.serialize(job);
        //                Rt.p("Dataload job size: "+bs.length);
        JobId jobId = hcc.startJob(deploymentId, job, EnumSet.of(JobFlag.PROFILE_RUNTIME));
        hcc.waitForCompletion(jobId);
        //        JobId jobId = hcc.createJob(app, job);
        //        hcc.start(jobId);
        //        hcc.waitForCompletion(jobId);
        return hcc.getJobStatus(jobId);
    }

    public JobStatus runDataGenerator(IIMRUDataGenerator generator) throws Exception {
        JobSpecification job = jobFactory.generateDataGenerateJob(generator);
        //                byte[] bs=JavaSerializationUtils.serialize(job);
        //                Rt.p("Data generator job size: "+bs.length);
        JobId jobId = hcc.startJob(deploymentId, job, EnumSet.of(JobFlag.PROFILE_RUNTIME));
        hcc.waitForCompletion(jobId);
        //        JobId jobId = hcc.createJob(app, job);
        //        hcc.start(jobId);
        //        hcc.waitForCompletion(jobId);
        return hcc.getJobStatus(jobId);
    }

    File explain(File path) {
        String s = path.toString();
        s = s.replaceAll(Pattern.quote("${NODE_ID}"), "local");
        return new File(s);
    }

    /**
     * Run the dataflow for a single IMRU iteration.
     * 
     * @param envInPath
     *            The HDFS path to read the current environment from.
     * @param envOutPath
     *            The DFS path to write the updated environment to.
     * @param iterationNum
     *            The iteration number.
     * @return The JobStatus of the job after completion or failure.
     * @throws Exception
     */
    private JobStatus runIMRUIteration(String modelName, int iterationNum) throws Exception {
        JobSpecification spreadjob = jobFactory.generateModelSpreadJob(deploymentId, modelName, iterationNum);
        //                byte[] bs=JavaSerializationUtils.serialize(spreadjob);
        //              Rt.p("IMRU job size: "+bs.length);
        JobId spreadjobId = hcc.startJob(deploymentId, spreadjob, EnumSet.of(JobFlag.PROFILE_RUNTIME));
        //        JobId jobId = hcc.createJob(app, job);
        //        hcc.start(jobId);
        hcc.waitForCompletion(spreadjobId);
        if (hcc.getJobStatus(spreadjobId) == JobStatus.FAILURE)
            return JobStatus.FAILURE;

        JobSpecification job = jobFactory.generateJob(imruSpec, iterationNum, modelName, noDiskCache);
        if (frameSize != 0)
            job.setFrameSize(frameSize);
        LOGGER.info("job frame size " + job.getFrameSize());
        //                byte[] bs=JavaSerializationUtils.serialize(job);
        //              Rt.p("IMRU job size: "+bs.length);
        JobId jobId = hcc.startJob(deploymentId, job, EnumSet.of(JobFlag.PROFILE_RUNTIME));
        //        JobId jobId = hcc.createJob(app, job);
        //        hcc.start(jobId);
        hcc.waitForCompletion(jobId);
        return hcc.getJobStatus(jobId);
    }

    /**
     * Write the model to a file.
     * 
     * @param model
     *            The model to write.
     * @param modelPath
     *            The DFS file to write the updated model to.
     * @throws IOException
     */
    private void writeModelToFile(Serializable model, File modelPath) throws IOException {
        modelPath = explain(modelPath);
        OutputStream fileOutput;
        File file = new File(modelPath.toString());
        if (!file.getParentFile().exists())
            file.getParentFile().mkdirs();
        fileOutput = new FileOutputStream(file);

        ObjectOutputStream output = new ObjectOutputStream(fileOutput);
        output.writeObject(model);
        output.close();
    }
}
