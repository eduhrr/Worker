import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;


public class mandarMens {

	private static AmazonEC2 ec2;
	private static String rowId;
	private static AmazonSQS sqs;

	private static void init() throws Exception {
		AWSCredentials credentials = new PropertiesCredentials(
				mandarMens.class
						.getResourceAsStream("AwsCredentials.properties"));
		setEc2(new AmazonEC2Client(credentials));
		setSqs(new AmazonSQSClient(credentials));
	}

	public static void main(String[] args) throws Exception {

		init();
		//setRowId(System.getenv("rowId"));
		setRowId("Esto es una prueba de que la cosa se ejecuta y va!!");

		try {
			Runnable newClient = new ClientWorkerThread(); 
	        Thread t = new Thread(newClient);
	        t.start(); 
			
			GetQueueUrlRequest qrequest = new GetQueueUrlRequest("iitLuna");
			String url = getSqs().getQueueUrl(qrequest).getQueueUrl();

			getSqs().sendMessage(new SendMessageRequest(url, getRowId())); // no
																			// hay
																			// variable..obvio
		} catch (AmazonServiceException ase) {
			System.out.println("Error Message:    " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:       " + ase.getErrorType());
			System.out.println("Request ID:       " + ase.getRequestId());
			killmePlease();
		} catch (AmazonClientException ace) {
			System.out.println("Error Message: " + ace.getMessage());
			killmePlease();
		}
		
		
		// Thread.sleep(30000); //sleep for 30 seg
		// Kill instance
		//killmePlease();
	}

	public static void killmePlease() {
		// Kill myself
		String instanceId = "";
		try {
			Process p = Runtime
					.getRuntime()
					.exec("sudo wget -q -O - http://169.254.169.254/latest/meta-data/instance-id");
			p.waitFor();
			BufferedReader br = new BufferedReader(new InputStreamReader(
					p.getInputStream()));
			instanceId = br.readLine();
		} catch (Exception e) {
			e.printStackTrace();
		}

		List<String> instancesToTerminate = new ArrayList<String>();
		instancesToTerminate.add(instanceId);

		TerminateInstancesRequest terminateRequest = new TerminateInstancesRequest();
		terminateRequest.setInstanceIds(instancesToTerminate);
		getEc2().terminateInstances(terminateRequest);
	}

	public static AmazonEC2 getEc2() {
		return ec2;
	}

	public static void setEc2(AmazonEC2 ec2) {
		mandarMens.ec2 = ec2;
	}

	public static String getRowId() {
		return rowId;
	}

	public static void setRowId(String rowId) {
		mandarMens.rowId = rowId;
	}

	public static AmazonSQS getSqs() {
		return sqs;
	}

	public static void setSqs(AmazonSQS sqs) {
		mandarMens.sqs = sqs;
	}

}
