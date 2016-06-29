package dsps1;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;

public class Manager {


	public static AmazonS3 s3;
	public static AmazonEC2 ec2;
	public static AmazonSQS sqs;
	public static String bucketName = "ishakim";

	public static void main(String[] args) throws MalformedURLException, IOException{
		boolean terminate = false;

		AWSCredentials credentials = null;
		try {
			credentials = new BasicAWSCredentials("CAN'T POST CREDENTIALS";);
		} catch (Exception e1) {
			throw new AmazonClientException("Please try again: Cannot load credentials" + e1);
		}
		try {
			System.out.println("Credentials created.");
			Region reg = Region.getRegion(Regions.US_EAST_1);
			s3 = new AmazonS3Client(credentials);
			s3.setRegion(reg);
			ec2 = new AmazonEC2Client(credentials);
			ec2.setRegion(reg);
			System.out.println("AmazonEC2Client created.");
			sqs = new AmazonSQSClient(credentials);
			sqs.setRegion(reg);	
			// connect to the queue from LocalApp
			System.out.println("connecting to the queue from LocalApp");
			String managerQueueUrl = sqs.getQueueUrl(new GetQueueUrlRequest("MainQueue")).getQueueUrl();
			System.out.println("succeeded");
			ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(managerQueueUrl);
			List<Message> msgs = sqs.receiveMessage(receiveMessageRequest).getMessages();

			//Creating the workers queue
			System.out.println("Manager: Creating the WorkersQueue");
			CreateQueueRequest createQueueRequest1 = new CreateQueueRequest("WorkersQueue");
			//String workerQueueUrl = sqs.createQueue(createQueueRequest1).getQueueUrl();
			String workerQueueUrl = sqs.createQueue(createQueueRequest1.addAttributesEntry("VisibilityTimeout", "180")).getQueueUrl();
			//Creating the summary queue
			System.out.println("Manager: Creating the WorkersSummaryQueue");
			CreateQueueRequest createQueueRequest2 = new CreateQueueRequest("WorkersSummary");
			//final String workersSummaryUrl = sqs.createQueue(createQueueRequest2).getQueueUrl();
			final String workersSummaryUrl = sqs.createQueue(createQueueRequest2.addAttributesEntry("VisibilityTimeout", "10")).getQueueUrl();

			int numOfWorkers = 0;
			int activeWorkers = 0;
			int numOfCurrentUrls = 0;
			final HashMap<String, Integer> localHash = new HashMap<String, Integer>();
			while(!terminate){
				if (!(msgs.isEmpty())){
					String msg = msgs.get(0).getBody();
					String messageRecieptHandle = msgs.get(0).getReceiptHandle();
					numOfCurrentUrls = 0;
					if (msg.equals("terminate")) {
						terminate = true;

						//Deleting the terminate message from localApp
						sqs.deleteMessage(new DeleteMessageRequest(managerQueueUrl, messageRecieptHandle));

						//Gets the last message to read
						msgs.clear();
						msgs = sqs.receiveMessage(receiveMessageRequest).getMessages();
						if (msgs.isEmpty()) break;
						else {
							msg = msgs.get(0).getBody();
							messageRecieptHandle = msgs.get(0).getReceiptHandle();
						}
					}

					String[] parsedMessage = msg.toString().split(",");
					String bucketName = parsedMessage[0];
					String inputFile = parsedMessage[1];
					int n = Integer.parseInt(parsedMessage[2]);
					final String local = parsedMessage[3];
					localHash.put(local, 0);

					//Downloads the input file from S3
					System.out.println("trying to get urls from " + bucketName);
					S3Object object = s3.getObject(new GetObjectRequest(bucketName, inputFile));
					BufferedReader input = new BufferedReader(new InputStreamReader(object.getObjectContent()));

					//Deleting the input message from localApp
					System.out.println("Manager: Deleting the input message");
					sqs.deleteMessage(new DeleteMessageRequest(managerQueueUrl, messageRecieptHandle));

					// insert urls to the workersQueue sqs as message contain Bucket name and 1 url as String.
					System.out.println("Manager: Sending messages to WorkersQueue");
					String Line = input.readLine();
					while (Line != null){
						String message = Line + "," + local;
						sqs.sendMessage(new SendMessageRequest(workerQueueUrl, message));
						Line = input.readLine();
						numOfCurrentUrls++;
					}
					localHash.put(local, numOfCurrentUrls);

					//Checks how many workers are there
					List<Reservation> reservList = ec2.describeInstances().getReservations();
					activeWorkers = 0;
					for (int i=0 ; i<reservList.size() ; i++){
						List<Instance> instances = reservList.get(i).getInstances();
						for (int j=0 ; j<instances.size() ; j++){
							if ((instances.get(j).getTags().size() > 0) && (instances.get(j).getTags().get(0).getValue().equals("Worker")) &&
									 ((instances.get(j).getState().getName().equals("running")) || (instances.get(j).getState().getName().equals("pending")))){
								
								activeWorkers++;
							}
						}
					}
					System.out.println("Manager: There are " + activeWorkers + " active workers now");

					//Starts Worker instances
					numOfWorkers = (numOfCurrentUrls / n);
					//edge case - 19 Worker instances can run on T2Micro
					if(numOfWorkers > 19){
						numOfWorkers = 19;
					}
					if(numOfWorkers > activeWorkers){
						numOfWorkers = numOfWorkers - activeWorkers;
					}
					else{
						numOfWorkers = 0;
					}

					System.out.println("Manager: I will add " + numOfWorkers + " more workers");
					
					int i = 0;
					while (i < numOfWorkers){
						try {
							RunInstancesRequest request = new RunInstancesRequest("ami-146e2a7c", 1, 1);
							request.setInstanceType(InstanceType.T2Micro.toString());
							request.setKeyName("ourkey");
							request.withUserData(workerScript());
							List<Instance> instances = ec2.runInstances(request).getReservation().getInstances();
							System.out.println("Launch instances: " + instances);
							//Tag the worker instance
							String resourceId = instances.get(0).getInstanceId();
							CreateTagsRequest createTagsRequest = new CreateTagsRequest();
							createTagsRequest.withResources(resourceId).withTags(new Tag("Label","Worker"));
							ec2.createTags(createTagsRequest);
							System.out.println("LocalApp: The " + (i+1) + " worker is on.");
							i++;
						} catch (AmazonServiceException e) {
							e.printStackTrace();
							System.out.println("Failed to launch the " + i + " instance: " + e);
						} catch (AmazonClientException e) {
							e.printStackTrace();
							System.out.println("Failed to launch the " + i + " instance: " + e);
						}
					}
					Thread t = new Thread(new Runnable() {
						public void run()
						{
							try {
								createFile(local, workersSummaryUrl, sqs, localHash);
							} catch (IOException e) {
								System.out.println("Manager: Thread failed to create file");
							}
						}
					});
					t.start();
				}
				if(!terminate){
					msgs.clear();
					msgs = sqs.receiveMessage(receiveMessageRequest).getMessages();
				}
			}


			// get all the attributes of the queue 
			List<String> attributeNames = new ArrayList<String>();
			attributeNames.add("All");

			// list the attributes of the queue we are interested in
			GetQueueAttributesRequest msgsInQRequest = new GetQueueAttributesRequest(workerQueueUrl);
			msgsInQRequest.setAttributeNames(attributeNames);
			Map<String, String> attributes = sqs.getQueueAttributes(msgsInQRequest).getAttributes();
			int messagesVisible = Integer.parseInt(attributes.get("ApproximateNumberOfMessages"));
			int messagesNotVisible = Integer.parseInt(attributes.get("ApproximateNumberOfMessagesNotVisible"));

			// do a loop until you find that workerQueueUrl is empty and only *then* send terminate massages
			while ((messagesVisible + messagesNotVisible) > 0){
				try {
					msgsInQRequest = new GetQueueAttributesRequest(workerQueueUrl);
					msgsInQRequest.setAttributeNames(attributeNames);
					attributes = sqs.getQueueAttributes(msgsInQRequest).getAttributes();
					messagesVisible = Integer.parseInt(attributes.get("ApproximateNumberOfMessages"));
					messagesNotVisible = Integer.parseInt(attributes.get("ApproximateNumberOfMessagesNotVisible"));
				} catch (Exception e) {
				}
			}

			//change the Visibility of the queue to 0
			HashMap<String, String> attributes2 = new HashMap<String, String>();
			attributes2.put("VisibilityTimeout", "0");
			sqs.setQueueAttributes(workerQueueUrl, attributes2);

			/*SetQueueAttributesRequest changeVisRequest = new SetQueueAttributesRequest(workerQueueUrl, attributes);
			changeVisRequest.addAttributesEntry("VisibilityTimeout", "0"); */

			System.out.println("Manager: send 'terminate' message to all the workers");
			int toTerminate = numOfWorkers + activeWorkers;
			for (int i = 0 ; i < toTerminate ; i++){
				sqs.sendMessage(new SendMessageRequest(workerQueueUrl, "terminate"));
			}

			try {
				Thread.sleep(80000);
			} catch (Exception e) {
				e.printStackTrace();
				System.out.println("Manager: couldn't sleep");
			}
			//Delete the workers queue, the workersSummary queue
			System.out.println("Manager: Deleting the WorkersQueue and the workersSummaryQueue");
			DeleteQueueRequest deleteQueueRequest1 = new DeleteQueueRequest(workerQueueUrl);
			sqs.deleteQueue(deleteQueueRequest1);
			DeleteQueueRequest deleteQueueRequest2 = new DeleteQueueRequest(workersSummaryUrl);
			sqs.deleteQueue(deleteQueueRequest2);
			List<Reservation> reserv = ec2.describeInstances().getReservations();
			List<Instance> instances = new ArrayList<Instance>();
			List<String> terminateInstancesIDs = new ArrayList<String>();
			for (int i=0 ; i<reserv.size() ; i++) {
				instances = reserv.get(i).getInstances();
				for (int j=0 ; j<instances.size() ; j++){
					if (instances.get(j).getTags().size() > 0 ){
						terminateInstancesIDs.add(instances.get(j).getInstanceId());
					}
				}
			}
			TerminateInstancesRequest terminateReq = new TerminateInstancesRequest(terminateInstancesIDs);
			System.out.println("Manager: terminate all instances");
			ec2.terminateInstances(terminateReq);
		} catch (AmazonServiceException e) {
			throw new AmazonServiceException("Please try again: Problem with Amazon Service " + e);
		} catch (NumberFormatException e) {
			throw new NumberFormatException("Please try again: Problem with NumberFormat " + e);
		} catch (IllegalArgumentException e) {
			throw new IllegalArgumentException("Please try again: Problem with using IllegalArgument " + e);
		} catch (AmazonClientException e) {
			throw new AmazonClientException("Please try again: Problem with AmazonClient " + e);
		}


	}

	private static void createFile(String local, String workersSummaryUrl, AmazonSQS sqs, HashMap<String, Integer> localHash) throws IOException{
		try {
			File file = new File("Summary - "+local);
			FileWriter fw = new FileWriter(file.getAbsoluteFile());
			BufferedWriter bw = new BufferedWriter(fw);
			StringBuilder builder = new StringBuilder();
			String body;
			String url;
			String localId;
			String group;
			String[] parts;
			StringBuilder thumbnail = new StringBuilder();
			thumbnail.append("Thumbnail\n");
			StringBuilder small = new StringBuilder();
			small.append("Small\n");
			StringBuilder medium = new StringBuilder();
			medium.append("Medium\n");
			StringBuilder large = new StringBuilder();
			large.append("Large\n");
			StringBuilder huge = new StringBuilder();
			huge.append("Huge\n");
			ReceiveMessageRequest req3 = new ReceiveMessageRequest(workersSummaryUrl);
			List<Message> resMsgs = sqs.receiveMessage(req3).getMessages();
			
			while(localHash.get(local) > 0){
				for (Message msg: resMsgs){
					body = msg.getBody();
					parts = body.split(",");
					if(parts.length == 2){
						String failMsg = parts[0];
						localId = parts[1];
						if(failMsg.equals("fail") && localId.equals(local)){
							sqs.deleteMessage(workersSummaryUrl, msg.getReceiptHandle());
							localHash.put(local, localHash.get(local) - 1);
							continue;
						}
						else continue;
					}
					else{
						url = parts[0];
						group = parts[1]; 
						localId = parts[2];
						if(localId.equals(local)){
							switch(group){
							case "Thumbnail":
								thumbnail.append(url+"\n");
								break;
							case "Small":
								small.append(url+"\n");
								break;
							case "Medium":
								medium.append(url+"\n");
								break;
							case "Large":
								large.append(url+"\n");
								break;
							case "Huge":
								huge.append(url+"\n");
								break;
							}
							sqs.deleteMessage(workersSummaryUrl, msg.getReceiptHandle());
							localHash.put(local, localHash.get(local) - 1);

						}
					}
				}
				resMsgs.clear();
				resMsgs = sqs.receiveMessage(req3).getMessages();
			}

			builder.append(thumbnail.toString());
			builder.append(small.toString());
			builder.append(medium.toString());
			builder.append(large.toString());
			builder.append(huge.toString());
			String content = builder.toString();
			bw.write(content);
			bw.close();
			PutObjectRequest por = new PutObjectRequest(bucketName, file.getName(), file);
			s3.putObject(por);
			System.out.println("File uploaded-manager");

			String managerDoneQueueUrl = sqs.getQueueUrl(new GetQueueUrlRequest("ManagerDoneQueue")).getQueueUrl();
			sqs.sendMessage(new SendMessageRequest(managerDoneQueueUrl, file.getName()));
		} catch (IOException e) {
			throw new IOException("Problem with creating the output file: " + e);
		}
	}

	private static String workerScript(){
		StringBuilder lines = new StringBuilder();
		lines.append("#! /bin/bash \n");
		lines.append("wget http://ishakim.s3.amazonaws.com/Worker.jar \n");
		lines.append("java -jar Worker.jar \n");
		String str = new String(Base64.encodeBase64(lines.toString().getBytes()));
		return str;
	}
}
