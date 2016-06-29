package dsps1;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.commons.codec.binary.Base64;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class LocalApplication{
	public static PropertiesCredentials Credentials;
	public static AmazonS3 s3;
	public static AmazonEC2 ec2;
	public static AmazonSQS sqs;
	public static String bucketName = "ishakim";
	public static String propertiesFilePath = "CAN'T POST CREDENTIALS";

	public static void main(String[] args) throws FileNotFoundException, IOException, InterruptedException{
		Credentials = new PropertiesCredentials(new FileInputStream(propertiesFilePath));
		Region reg = Region.getRegion(Regions.US_EAST_1);
		System.out.println("Credentials created");
		ec2 = new AmazonEC2Client(Credentials);
		ec2.setRegion(reg);
		System.out.println("AmazonEC2Client created");
		Boolean managerExists = false;
		List<Instance> instances = new ArrayList<Instance>();
		List<String> instencesIds= new ArrayList<String>();
		UUID localId = UUID.randomUUID();
		String mainQueueUrl = "";
		String managerDoneQueueUrl = "";
		sqs = new AmazonSQSClient(Credentials);
		sqs.setRegion(reg);	

		//Checks if manager exists
		try {
			List<Reservation> reservations = ec2.describeInstances().getReservations();
			for (int i=0 ; i<reservations.size() && !managerExists ; i++) {
				instances = reservations.get(i).getInstances();
				for (int j=0 ; j<instances.size() ; j++){
					if (instances.get(j).getTags().size() > 0 && 
							instances.get(j).getTags().get(0).getValue().equals("Manager") && 
							instances.get(j).getState().getName().equals("running")){
						managerExists = true;
						System.out.println("Local: using an existing instance");
						break;
					}
				}
			}

			//Creates a manager
			if(!managerExists){
				RunInstancesRequest request = new RunInstancesRequest("ami-146e2a7c", 1, 1);
				request.setInstanceType(InstanceType.T2Micro.toString());
				request.setKeyName("ourkey");
				request.withUserData(managerScript()); 
				instances = ec2.runInstances(request).getReservation().getInstances();
				System.out.println("Launch instances: " + instances);
				instencesIds.add(instances.get(0).getInstanceId());

				//Tag the manager instance
				String resourceId = instances.get(0).getInstanceId();
				CreateTagsRequest createTagsRequest = new CreateTagsRequest();
				createTagsRequest.withResources(resourceId).withTags(new Tag("Label","Manager"));
				ec2.createTags(createTagsRequest);
				System.out.println("Local:  The Manager is on");
				System.out.println("Creating a new SQS queue called MainQueue");

				CreateQueueRequest main = new CreateQueueRequest("MainQueue");
				//String mainQueueUrl = sqs.createQueue(main).getQueueUrl();
				mainQueueUrl = sqs.createQueue(main.addAttributesEntry("VisibilityTimeout", "0")).getQueueUrl();
				System.out.println("Creating a new SQS queue called managerDoneQueue");
				CreateQueueRequest summary = new CreateQueueRequest("ManagerDoneQueue");
				managerDoneQueueUrl = sqs.createQueue(summary).getQueueUrl();
			}
			else{
				mainQueueUrl = sqs.getQueueUrl(new GetQueueUrlRequest("MainQueue")).getQueueUrl();
				managerDoneQueueUrl = sqs.getQueueUrl(new GetQueueUrlRequest("ManagerDoneQueue")).getQueueUrl();
			}
		} catch (AmazonServiceException ase) {
			System.out.println("Caught Exception: " + ase.getMessage());
			System.out.println("Reponse Status Code: " + ase.getStatusCode());
			System.out.println("Error Code: " + ase.getErrorCode());
			System.out.println("Request ID: " + ase.getRequestId());
		}

		s3 = new AmazonS3Client(Credentials);
		s3.setRegion(reg);
		System.out.println("AmazonS3Client created.");
		File f = new File(args[0]);
		//If the bucket doesn't exist - will create it.
		if (!s3.doesBucketExist(bucketName)) {
			s3.createBucket(bucketName);
		}
		System.out.println("Bucket exist.");
		PutObjectRequest por = new PutObjectRequest(bucketName, f.getName(), f);
		//Uploads the file
		s3.putObject(por);
		System.out.println("Input uploaded.");


		System.out.println("Sending a message to MainQueue");
		String msgManager = bucketName + "," + args[0] + "," + args[2] + "," + localId;
		sqs.sendMessage(new SendMessageRequest(mainQueueUrl, msgManager));

		if (args.length > 3){
			if(args[3].equals("terminate")){
				sqs.sendMessage(new SendMessageRequest(mainQueueUrl, "terminate"));      
			}
		}


		System.out.println("Local: connect to queue");
		try {
			ReceiveMessageRequest req = new ReceiveMessageRequest(managerDoneQueueUrl);
			List<Message> msgs = sqs.receiveMessage(req).getMessages();
			boolean done = false;
			while (!done){
				for(Message msg: msgs){

					String name = msg.getBody();
					if(name.equals("Summary - "+(localId.toString()))){
						done = true;
						S3Object output = s3.getObject(new GetObjectRequest(bucketName, name));
						String outputName = args[1];
						convertToHtml(output.getObjectContent(), outputName, localId.toString());
						sqs.deleteMessage(managerDoneQueueUrl, msg.getReceiptHandle());
					}
					else continue;
				}
				if(!done){
					try {
						msgs = sqs.receiveMessage(req).getMessages();
					} catch (Exception e) {
					}
				}
			}
		} catch (AmazonServiceException e1) {
			throw new AmazonServiceException ("Problem accessing the ManagerDone queue: " + e1);
		} catch (AmazonClientException e1) {
			throw new AmazonClientException ("Problem accessing the ManagerDone queue: " + e1);
		}
		
		if (args.length > 3){
			if (args[3].equals("terminate")){
				try {
					DeleteQueueRequest deleteQueueRequest1 = new DeleteQueueRequest(mainQueueUrl);
					sqs.deleteQueue(deleteQueueRequest1);
				} catch (AmazonServiceException e) {
					System.out.println("MainQueue has been deleted");
				} catch (AmazonClientException e) {
					System.out.println("MainQueue has been deleted");
				}
			}
		}
		System.out.println("Local: The MainQueue has been deleted");
	}

	private static void convertToHtml(S3ObjectInputStream input, String name, String localId) throws IOException{
		String fileName = name.replaceAll(".txt", ".html");
		BufferedReader reader = new BufferedReader(new InputStreamReader(input));
		String line;
		StringBuilder builder1 = new StringBuilder();
		StringBuilder builder2 = new StringBuilder();
		StringBuilder builder3 = new StringBuilder();
		StringBuilder builder4 = new StringBuilder();
		StringBuilder builder5 = new StringBuilder();
		line = reader.readLine();
		while (line != null){
			if(line.equals("Thumbnail")){
				line = processLines(reader, "Small", builder1);
			}
			else if(line.equals("Small")){
				line = processLines(reader, "Medium", builder2);
			}
			else if(line.equals("Medium")){
				line = processLines(reader, "Large", builder3);
			}
			else if(line.equals("Large")){
				line = processLines(reader, "Huge", builder4);
			}
			else{
				line = reader.readLine();
				while(line != null){
					builder5.append("<a href="+line+"><img src='"+line+"'></img></a></br>");
					line = reader.readLine();
				}
			}
		}
		File dir = new File(localId);
		dir.mkdir();
		String dirName = dir.getName();
		File file = new File(dirName+"/"+fileName);
		FileWriter fw = new FileWriter(file.getAbsoluteFile());
		BufferedWriter bw = new BufferedWriter(fw);
		StringBuilder builder = new StringBuilder();
		builder.append("<html>");
		builder.append("<head>");
		builder.append("<h1>Group Links");
		builder.append("<p><a href="+createUrl("Thumbnail", dirName, builder1)+" target=_blank"+">Thumbnail</a></p>");
		builder.append("<p><a href="+createUrl("Small", dirName, builder2)+" target=_blank"+">Small</a></p>");
		builder.append("<p><a href="+createUrl("Medium", dirName, builder3)+" target=_blank"+">Medium</a></p>");
		builder.append("<p><a href="+createUrl("Large", dirName, builder4)+" target=_blank"+">Large</a></p>");
		builder.append("<p><a href="+createUrl("Huge", dirName, builder5)+" target=_blank"+">Huge</a></p>");
		builder.append("</h1>");
		builder.append("</head>");
		builder.append("<body> <b>Daniel Grinberg, Asaf Mazali</b>");
		builder.append("</body>");
		builder.append("</html>");
		String content = builder.toString();
		bw.write(content);
		bw.close();
	}

	private static String processLines(BufferedReader reader, String category, StringBuilder builder) throws IOException{
		String line = reader.readLine();
		while(!(line.equals(category))){
			builder.append("<a href="+line+"><img src='"+line+"'></img></a></br>");
			line = reader.readLine();
		}
		return line;
	}

	private static String createUrl(String group, String dirName, StringBuilder builder) throws IOException{
		File file = new File(dirName+"/"+group+".html");
		StringBuilder builder1 = new StringBuilder();
		builder1.append("<html>");
		builder1.append("<body>");
		builder1.append(builder.toString());
		builder1.append("</body>");
		builder1.append("</html>");
		FileWriter fw = new FileWriter(file.getAbsoluteFile());
		BufferedWriter bw = new BufferedWriter(fw);
		String content = builder1.toString();
		bw.write(content);
		bw.close();
		return file.getAbsolutePath();
	}

	private static String managerScript(){
		StringBuilder lines = new StringBuilder();
		lines.append("#! /bin/bash \n");
		lines.append("wget http://ishakim.s3.amazonaws.com/Manager.jar \n");
		lines.append("java -jar Manager.jar \n");
		String str = new String(Base64.encodeBase64(lines.toString().getBytes()));
		return str;
	}
}