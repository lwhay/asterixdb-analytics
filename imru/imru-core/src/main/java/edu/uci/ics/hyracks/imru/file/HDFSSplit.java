package edu.uci.ics.hyracks.imru.file;


//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.mapreduce.InputSplit;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.lib.input.FileSplit;
//import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

/**
 * Separate hadoop classes to avoid uploading
 * hadoop.jar when HDFS is not used.
 * Doesn't work as expected.
 * 
 * @author wangrui
 */
//public class HDFSSplit implements Serializable {
//    ConfigurationFactory confFactory;
//    String path;
//    String[] locations;
//
//    public HDFSSplit(ConfigurationFactory confFactory, String path)
//            throws IOException, InterruptedException {
//        this.confFactory = confFactory;
//        this.path = path;
//    }
//
//    public HDFSSplit(String path) throws IOException {
//        this.path = path;
//    }
//
//    public String[] getLocations() throws IOException {
//        if (locations == null) {
//         // Use a dummy input format to create a list of
//            // InputSplits for the
//            // input files
//            Job dummyJob = new Job(confFactory.createConfiguration());
//            TextInputFormat.addInputPaths(dummyJob, path);
//            // Disable splitting of files:
//            TextInputFormat.setMinInputSplitSize(dummyJob, Long.MAX_VALUE);
//            try {
//                locations = new TextInputFormat().getSplits(dummyJob).get(0)
//                        .getLocations();
//            } catch (InterruptedException e) {
//                throw new IOException(e);
//            }
//        }
//        return locations;
//    }
//
//    public static List<IMRUFileSplit> get(ConfigurationFactory confFactory,
//            String[] splits) throws IOException, InterruptedException {
//        Vector<IMRUFileSplit> list = new Vector<IMRUFileSplit>(splits.length);
//        for (String split : splits)
//            list.add(new IMRUFileSplit(new HDFSSplit(confFactory, split)));
//        return list;
//    }
//
//    public InputStream getInputStream() throws IOException {
//        Path path = new Path(this.path);
//        Configuration conf = confFactory.createConfiguration();
//        FileSystem dfs = FileSystem.get(conf);
//        return HDFSUtils.open(dfs, conf, path);
//    }
//
//    public boolean isDirectory() throws IOException {
//        Path path = new Path(this.path);
//        Configuration conf = confFactory.createConfiguration();
//        FileSystem dfs = FileSystem.get(conf);
//        return dfs.isDirectory(path);
//    }
//}
