package edu.uci.ics.hyracks.imru.example.kmeans;

public class KMeansEC2 {
    public static void main(String[] args) throws Exception {
        // File home = new File(System.getProperty("user.home"));
        // File credentialsFile = new File(home, "AwsCredentials.properties");
        // File privateKey = new File(home, "firstTestByRui.pem");
        // File hyracksEc2Root = new File(home,
        // "hyracks/hyracks/hyracks-ec2/target/appassembler");
        // String exampleData = System.getProperty("user.home") +
        // "/hyracks/imru/imru-example/data/kmeans";
        // boolean setupClusterFirst = true;
        // boolean uploadData = true;
        // // setupClusterFirst = false;
        // // uploadData = false;
        // int dataSplits = 2;
        // String[] localPaths = new String[dataSplits];
        // for (int i = 0; i < dataSplits; i++)
        // localPaths[i] = exampleData + "/kmeans" + i + ".txt";
        // ImruEC2 ec2 = new ImruEC2(credentialsFile, privateKey);
        // if (setupClusterFirst)
        // ec2.setup(hyracksEc2Root, 2, "t1.micro");
        // String path;
        // if (uploadData)
        // path = ec2.uploadData(localPaths, "kmeans");
        // else
        // path = ec2.getSuggestedLocations(localPaths, "kmeans");
        // int k = 3;
        //
        // double minDis = Double.MAX_VALUE;
        // KMeansModel bestModel = null;
        // for (int modelId = 0; modelId < 3; modelId++) {
        // System.out.println("trial " + modelId);
        // KMeansModel initModel = ec2.run(new RandomSelectJob(k), new
        // KMeansModel(k, 1), "kmeans", path);
        // System.out.println("InitModel: " + initModel);
        // initModel.roundsRemaining = 20;
        // KMeansModel finalModel = ec2.run(new KMeansJob(k), initModel,
        // "kmeans", path);
        // System.out.println("FinalModel: " + finalModel);
        // System.out.println("DistanceSum: " + finalModel.lastDistanceSum);
        // if (finalModel.lastDistanceSum < minDis) {
        // minDis = finalModel.lastDistanceSum;
        // bestModel = finalModel;
        // }
        // }
        // System.out.println("BestModel: " + bestModel);
        // System.exit(0);
    }
}
