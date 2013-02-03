import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Date;
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
	private static String rowID;
	private static AmazonSQS sqs;

	private static void init() throws Exception {
		AWSCredentials credentials = new PropertiesCredentials(
				mandarMens.class
						.getResourceAsStream("AwsCredentials.properties"));
		setEc2(new AmazonEC2Client(credentials));
		setSqs(new AmazonSQSClient(credentials));
	}

	public static void main(String[] args) throws Exception {
		logging("Luna Worker Script");
		logging("Developed by Eduardo Hernandez Marquina, Hector Veiga and Gerardo Travesedo");
		logging("");
		init();
		BufferedReader bf = new BufferedReader(new FileReader("rowID.txt"));
		String rowIDaux="";
//		String strLinea="";
//        // read the file line by line
//        while ((strLinea = bf.readLine()) != null)   {
//            rowIDaux= rowIDaux+strLinea;
//        }
		rowIDaux = bf.readLine();
        bf.close();
		setRowId(rowIDaux);
		logging("Id of row of request retrieved: " + rowID);
//		setRowId("Esto es una prueba de que la cosa se ejecuta y va!!");

		try {
//			Runnable newClient = new ClientWorkerThread(); 
//	        Thread t = new Thread(newClient);
//	        t.start(); 
			
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
		
		
		//Thread.sleep(30000); //sleep for 30 seg
		Thread.sleep(100*60*15); //sleep for 15 min
		//Kill instance
		killmePlease();
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
	
//	private static String getInstanceType() {
//		String instanceType = "";
//		try {
//			Process p = Runtime
//					.getRuntime()
//					.exec("sudo wget -q -O - http://169.254.169.254/latest/meta-data/instance-type");
//			p.waitFor();
//			BufferedReader br = new BufferedReader(new InputStreamReader(
//					p.getInputStream()));
//			instanceType = br.readLine();
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		return instanceType;
//	}

	public static String substringBetween(String str, String open, String close) {
		if (str == null || open == null || close == null) {
			return null;
		}
		int start = str.indexOf(open);
		if (start != -1) {
			int end = str.indexOf(close, start + open.length());
			if (end != -1) {
				return str.substring(start + open.length(), end);
			}
		}
		return null;
	}

	public static int giveMeSeconds(String line, String del1, String del2) {
		String durVid = "";
		String[] durVidPieces;

		durVid = substringBetween(line, del1, del2);
		durVidPieces = durVid.split(":");
		int secs = (Integer.parseInt(durVidPieces[0]) * 3600)
				+ (Integer.parseInt(durVidPieces[1]) * 60)
				+ Integer.parseInt(durVidPieces[2].substring(0, 2));
		return secs;
	}

//	private static void updateStatus(String percentage) {
//		// Update Database Status
//		try {
//			Connection connec = DriverManager.getConnection(
//					"jdbc:mysql://64.131.110.162/luna", "Europa", "a23d578");
//			Statement s = connec.createStatement();
//			String query1 = "UPDATE requests SET status='" + percentage
//					+ "' WHERE id='" + rowId + "'";
//			s.executeUpdate(query1);
//			s.close();
//			connec.close();
//		} catch (Exception e) {
//			logging("Exception catch!!!! Something wrong updating status of a request");
//			logging(e.toString());
//		}
//	}

//	private static void updateParameter(String parameter, long quantity) {
//		logging("Updating parameter '" + parameter + "' by " + quantity);
//
//		// Update Database Parameter
//		try {
//			Connection connec = DriverManager.getConnection(
//					"jdbc:mysql://64.131.110.162/luna", "Europa", "a23d578");
//			Statement s = connec.createStatement();
//			String query1 = "UPDATE parameters SET value=value+"
//					+ String.valueOf(quantity) + " WHERE parameter='"
//					+ parameter + "'";
//			s.executeUpdate(query1);
//			s.close();
//			connec.close();
//		} catch (Exception e) {
//			logging("Exception catch!!!! Something wrong updating status of a request");
//			logging(e.toString());
//		}
//	}

	private static void logging(String lineToLog) {
		Date time = new Date();
		String line = "[" + time.toString() + "] " + lineToLog;
		System.out.println(line);
	}



	public static AmazonEC2 getEc2() {
		return ec2;
	}

	public static void setEc2(AmazonEC2 ec2) {
		mandarMens.ec2 = ec2;
	}

	public static String getRowId() {
		return rowID;
	}

	public static void setRowId(String rowId) {
		mandarMens.rowID = rowId;
	}

	public static AmazonSQS getSqs() {
		return sqs;
	}

	public static void setSqs(AmazonSQS sqs) {
		mandarMens.sqs = sqs;
	}

}
