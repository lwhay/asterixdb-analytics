package edu.uci.ics.hyracks.imru.data;

import java.io.File;
import java.util.EnumSet;
import java.util.logging.Logger;

import org.apache.hyracks.api.client.HyracksConnection;
import org.apache.hyracks.api.deployment.DeploymentId;
import org.apache.hyracks.api.job.JobFlag;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.api.job.JobStatus;

import edu.uci.ics.hyracks.imru.jobgen.IMRUJobFactory;
import edu.uci.ics.hyracks.imru.runtime.bootstrap.IMRUConnection;
import edu.uci.ics.hyracks.imru.util.Rt;

/**
 * Distribute data to the cluster in logN steps
 * 
 * @author Rui Wang
 */
public class DataSpreadDriver {
    private final static Logger LOGGER = Logger.getLogger(DataSpreadDriver.class.getName());
    private final HyracksConnection hcc;
    DeploymentId deploymentId;
    private final IMRUConnection imruConnection;
    private final String app;

    File file;
    public String[] targetNodes;
    String targetPath;

    public DataSpreadDriver(HyracksConnection hcc, DeploymentId deploymentId, IMRUConnection imruConnection,
            String app, File file, String[] targetNodes, String targetPath) {
        this.hcc = hcc;
        this.deploymentId = deploymentId;
        this.imruConnection = imruConnection;
        this.app = app;
        this.file = file;
        this.targetNodes = targetNodes;
        this.targetPath = targetPath;
    }

    public JobStatus run() throws Exception {
        Rt.p("uploading " + file.getAbsolutePath());
        imruConnection.uploadData(file.getName(), Rt.readFileByte(file));
        Rt.p("uploaded");

        JobSpecification spreadjob = IMRUJobFactory.generateModelSpreadJob(deploymentId, targetNodes, targetNodes[0],
                imruConnection, file.getName(), 0, targetPath);
        JobId spreadjobId = hcc.startJob(deploymentId, spreadjob, EnumSet.of(JobFlag.PROFILE_RUNTIME));
        hcc.waitForCompletion(spreadjobId);
        JobStatus status = hcc.getJobStatus(spreadjobId);
        long loadEnd = System.currentTimeMillis();
        return status;
    }
}
