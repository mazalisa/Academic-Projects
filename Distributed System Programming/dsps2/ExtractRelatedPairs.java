package dsps2;

import java.io.IOException;
import java.util.ArrayList;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.PlacementType;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;


public class ExtractRelatedPairs {
	
	public static final String bucketName = "ishakim";
	public static final String accessKey = "AKIAIY5357BY4TEG6K4Q";
	public static final String secretKey = "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";
	public static final String dataSet = "s3n://dsp112/eng.corp.10k";

	
	public static void main(String[] args) throws IOException {
		
		AWSCredentials credentials = null;
		try {
			credentials = new BasicAWSCredentials(accessKey, secretKey);
		} catch (Exception e1) {
			throw new AmazonClientException("Please try again: Cannot load credentials" + e1);
		}
		AmazonElasticMapReduce mapReduce = new AmazonElasticMapReduceClient(credentials);
		
		// Step 1
		HadoopJarStepConfig hadoopJarStep1 = new HadoopJarStepConfig()
		    .withJar("s3n://"+ bucketName +"/Step1.jar")
		    .withArgs(dataSet, "s3n://"+ bucketName +"/output/output1");
		
		StepConfig stepConfig1 = new StepConfig()
			.withName("Step1")
			.withHadoopJarStep(hadoopJarStep1)
			.withActionOnFailure("TERMINATE_JOB_FLOW");
		
		// Step 2
		HadoopJarStepConfig hadoopJarStep2 = new HadoopJarStepConfig()
			.withJar("s3n://"+ bucketName +"/Step2.jar")
			.withArgs("s3n://"+ bucketName +"/output/output1","s3n://"+ bucketName +"/output/output2");
	
		StepConfig stepConfig2 = new StepConfig()
			.withName("Step2")
			.withHadoopJarStep(hadoopJarStep2)
			.withActionOnFailure("TERMINATE_JOB_FLOW");
		
		// Step 3
		HadoopJarStepConfig hadoopJarStep3 = new HadoopJarStepConfig()
			.withJar("s3n://"+ bucketName +"/Step3.jar")
			.withArgs("s3n://"+ bucketName +"/output/output2","s3n://"+ bucketName +"/output/output3");

		StepConfig stepConfig3 = new StepConfig()
			.withName("Step3")
			.withHadoopJarStep(hadoopJarStep3)
			.withActionOnFailure("TERMINATE_JOB_FLOW");
		
		// Step 4
		HadoopJarStepConfig hadoopJarStep4 = new HadoopJarStepConfig()
		.withJar("s3n://"+ bucketName +"/Step4.jar")
		.withArgs("s3n://"+ bucketName +"/output/output3","s3n://"+ bucketName +"/output/output_final",args[0]);

		StepConfig stepConfig4 = new StepConfig()
			.withName("Step4")
			.withHadoopJarStep(hadoopJarStep4)
			.withActionOnFailure("TERMINATE_JOB_FLOW");
		
		ArrayList<StepConfig> steps = new ArrayList<StepConfig>();
		steps.add(stepConfig1);
		steps.add(stepConfig2);
		steps.add(stepConfig3);
		steps.add(stepConfig4);
		
		JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
		    .withInstanceCount(19)
		    .withMasterInstanceType(InstanceType.M1Medium.toString())
		    .withSlaveInstanceType(InstanceType.M1Medium.toString())
		    .withHadoopVersion("2.4.0")
		    .withEc2KeyName("ourkey")
		    .withKeepJobFlowAliveWhenNoSteps(false)
		    .withPlacement(new PlacementType("us-east-1b"));
		
		RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
		    .withName("ExtractRelatedPairs")
		    .withInstances(instances)
		    .withSteps(steps)
		    .withLogUri("s3n://"+ bucketName +"/Logs/")
		    .withAmiVersion("3.1.0")
		    .withServiceRole("EMR_DefaultRole")
		    .withJobFlowRole("EMR_EC2_DefaultRole");
		    
		
		
		RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
		String jobFlowId = runJobFlowResult.getJobFlowId();
		System.out.println("Ran job flow with id: " + jobFlowId);
	}

}
