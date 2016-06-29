package dsps1;

import java.awt.image.BufferedImage;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import javax.imageio.ImageIO;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class Worker {

	public static PropertiesCredentials Credentials;
	public static AmazonS3 s3;
	public static AmazonEC2 ec2;
	public static AmazonSQS sqs;
	public static String bucketName = "ishakim";


	public static void main(String[] args) throws Exception{
		try{
			AWSCredentials credentials = new BasicAWSCredentials("AKIAIKIEVLKALUG2JY7Q", "hTx+2zySum8M0N70HaaJSThinUW5WwYjXT9wN7nZ");
			Region reg = Region.getRegion(Regions.US_EAST_1);
			s3 = new AmazonS3Client(credentials);
			s3.setRegion(reg);
			System.out.println("AmazonS3Client created-worker");
			sqs = new AmazonSQSClient(credentials);
			sqs.setRegion(reg);	
			System.out.println("AmazonSQSClient created-worker");
			String workersQueueUrl = sqs.getQueueUrl(new GetQueueUrlRequest("WorkersQueue")).getQueueUrl();
			String workersSummaryUrl = sqs.getQueueUrl(new GetQueueUrlRequest("WorkersSummary")).getQueueUrl();
			System.out.println("Connect to queues-worker");
			ReceiveMessageRequest req = new ReceiveMessageRequest(workersQueueUrl);
			List<Message> msgs = sqs.receiveMessage(req).getMessages();
			System.out.println("Recieve messages-worker");
			ArrayList<String> succ_urls = new ArrayList<String>();
			ArrayList<String> fail_urls = new ArrayList<String>();
			String id = retrieveInstanceId();
			Date start_time = new Date();
			Boolean terminate = false;
			int counter = 0;
			long average = 0;
			long total = 0;
			while (!terminate){
				for(Message msg: msgs){
					String recipt = msg.getReceiptHandle();
					long start = System.currentTimeMillis();
					String message = msg.getBody();
					if(message.equals("terminate")){
						terminate = true;
						break;
					}
					String[] parsedMessage = message.toString().split(",");
					String body = parsedMessage[0];
					String local = parsedMessage[1];
					String processedMsg = readImage(body, succ_urls, fail_urls);
					if(processedMsg.equals("fail")){
						sqs.sendMessage(new SendMessageRequest(workersSummaryUrl, "fail"+","+local));
						try {
							sqs.deleteMessage(new DeleteMessageRequest(workersQueueUrl, recipt));
						} catch (Exception e) {
						}
						continue;
					}

					//sends done to summary queue
					sqs.sendMessage(new SendMessageRequest(workersSummaryUrl, processedMsg+","+local));
					//deletes msg from workers queue
					try {
						sqs.deleteMessage(new DeleteMessageRequest(workersQueueUrl, recipt));
					} catch (Exception e) {
						e.printStackTrace();
					}
					long finish = System.currentTimeMillis();
					counter++;
					total = total + (finish - start);
				}
				if(!terminate){
					msgs.clear();
					try {
						msgs = sqs.receiveMessage(req).getMessages();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
			if(counter > 0){
				average = (long) (total / counter);
			}
			Date finish_time = new Date();
			File file = new File("Worker_"+id);
			FileWriter fw = new FileWriter(file.getAbsoluteFile());
			BufferedWriter bw = new BufferedWriter(fw);
			StringBuilder builder = new StringBuilder();
			builder.append("id: "+id+"\n");
			builder.append("Start time: "+start_time.toString()+"\n");
			builder.append("Average run-time on a single url: "+average+"\n");
			builder.append("List of all the successful urls: Amount - "+succ_urls.size()+"\n");
			for(int i=0; i<succ_urls.size(); i++){
				builder.append("\t"+succ_urls.get(i).toString()+"\n");
			}
			builder.append("List of all the failed urls: Amount - "+fail_urls.size()+"\n");
			for(int i=0; i<fail_urls.size(); i++){
				builder.append("\t"+fail_urls.get(i).toString()+"\n");
			}
			builder.append("Total number of urls it handles: "+(succ_urls.size()+fail_urls.size())+"\n");
			builder.append("Finish time: "+finish_time.toString()+"\n");
			String content = builder.toString();
			bw.write(content);
			bw.close();
			PutObjectRequest por = new PutObjectRequest(bucketName, file.getName(), file);
			s3.putObject(por);
			System.out.println("File uploaded-worker");
		} catch (AmazonServiceException ase) {
			System.out.println("Caught Exception: " + ase.getMessage());
			System.out.println("Reponse Status Code: " + ase.getStatusCode());
			System.out.println("Error Code: " + ase.getErrorCode());
			System.out.println("Request ID: " + ase.getRequestId());
		}
	}

	private static String readImage(String url, ArrayList<String> succ_urls, ArrayList<String> fail_urls) throws Exception{
		String msg = url + ",";
		try{
			CloseableHttpClient httpclient = HttpClients.createDefault(); 
			HttpGet httpget = new HttpGet(url);
			CloseableHttpResponse response = httpclient.execute(httpget);
			InputStream stream = response.getEntity().getContent(); 
			BufferedImage bimg = ImageIO.read(stream);
			int width          = bimg.getWidth();
			int height         = bimg.getHeight();
			succ_urls.add(url.toString());
			int check = width*height;
			if(check < 64*64){msg = msg + "Thumbnail";}
			else if(check < 256*256){msg = msg + "Small";}
			else if(check < 640*480){msg = msg + "Medium";}
			else if(check < 1024*768){msg = msg + "Large";}
			else msg = msg + "Huge";

		}
		catch (Exception e){
			fail_urls.add(url+"\n\tException: "+e);
			msg = "fail";
		}
		return msg;

	}
	
	private static String retrieveInstanceId() throws IOException {
	    String EC2Id = null;
	    String inputLine;
	    URL EC2MetaData = new URL("http://169.254.169.254/latest/meta-data/instance-id");
	    URLConnection EC2MD = EC2MetaData.openConnection();
	    BufferedReader in = new BufferedReader(new InputStreamReader(EC2MD.getInputStream()));
	    while ((inputLine = in.readLine()) != null) {
	        EC2Id = inputLine;
	    }
	    in.close();
	    return EC2Id;
	}

}