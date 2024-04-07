import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;

import java.util.ArrayList;
import java.util.List;

public class App {
    public static AWSCredentialsProvider credentialsProvider;
    public static AmazonS3 S3;
    public static AmazonEC2 ec2;
    public static AmazonElasticMapReduce emr;

    public static int numberOfInstances = 8;

    public static void main(String[] args) {
        credentialsProvider = new ProfileCredentialsProvider();
        System.out.println("[INFO] Connecting to AWS");
        ec2 = AmazonEC2ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-east-1")
                .build();
        S3 = AmazonS3ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-east-1")
                .build();
        emr = AmazonElasticMapReduceClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-east-1")
                .build();

        String minPmi = args[1];
        String relMinPmi = args[2];

        // Step 1
        HadoopJarStepConfig step1Config = new HadoopJarStepConfig()
                .withJar("s3://this-is-my-first-bucket-dn2/jars/Step1.jar")
                .withMainClass("Step1");

        StepConfig step1 = new StepConfig()
                .withName("Step1")
                .withHadoopJarStep(step1Config)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        // Step 2
        HadoopJarStepConfig step2Config = new HadoopJarStepConfig()
                .withJar("s3://this-is-my-first-bucket-dn2/jars/Step2.jar")
                .withMainClass("Step2");

        StepConfig step2 = new StepConfig()
                .withName("Step2")
                .withHadoopJarStep(step2Config)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        // Step 3
        HadoopJarStepConfig step3Config = new HadoopJarStepConfig()
                .withJar("s3://this-is-my-first-bucket-dn2/jars/Step3.jar")
                .withMainClass("Step3");

        StepConfig step3 = new StepConfig()
                .withName("Step3")
                .withHadoopJarStep(step3Config)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        // Step 4
        HadoopJarStepConfig step4Config = new HadoopJarStepConfig()
                .withJar("s3://this-is-my-first-bucket-dn2/jars/Step4.jar")
                .withMainClass("Step4")
                .withArgs(minPmi, relMinPmi);

        StepConfig step4 = new StepConfig()
                .withName("Step4")
                .withHadoopJarStep(step4Config)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        // Step 5
        HadoopJarStepConfig step5Config = new HadoopJarStepConfig()
                .withJar("s3://this-is-my-first-bucket-dn2/jars/Step5.jar")
                .withMainClass("Step5");

        StepConfig step5 = new StepConfig()
                .withName("Step5")
                .withHadoopJarStep(step5Config)
                .withActionOnFailure("TERMINATE_JOB_FLOW");


        // Job flow
        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(numberOfInstances)
                .withMasterInstanceType(InstanceType.M4Xlarge.toString())
                .withSlaveInstanceType(InstanceType.M4Xlarge.toString())
                .withHadoopVersion("2.9.2")
                .withEc2KeyName("vockey")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));

        List<StepConfig> steps = new ArrayList<>();
        steps.add(step1);
        steps.add(step2);
        steps.add(step3);
        steps.add(step4);
        steps.add(step5);

        System.out.println("Setting up job flow");
        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("MapReduce Project")
                .withInstances(instances)
                .withSteps(steps)
                .withLogUri("s3://this-is-my-first-bucket-dn2/logs/")
                .withServiceRole("EMR_DefaultRole")
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withReleaseLabel("emr-5.11.0");

        RunJobFlowResult runJobFlowResult = emr.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);
    }
}
