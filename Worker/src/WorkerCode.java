/**
 * Remember to fill out the AWS credentials file and put it in the top folder 
 * of the java program. Ii could also be needed to change the serverName and 
 * the port number to the appropriate ones where Lunacore is listening.  
 */
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;

/**
 * Main class of the java Worker code. It calls the rendering thread and the
 * communication thread. It also terminates the Amazon EC2 instance in case of
 * malfunction
 * 
 * @author Eduardo Hernandez Marquina
 * 
 */
public class WorkerCode {

	private static AmazonEC2 ec2;
	// ServerName of the LunaCore server, for the socket connection
	private static final String serverName = "54.243.226.19";
	// Port number for the socket connection
	private static final int port = 6060;

	/**
	 * Static initializer block for setting up the AWS credentials which should
	 * be in the top folder.
	 * 
	 */
	static {
		AWSCredentials credentials;
		try {
			credentials = new PropertiesCredentials(
					WorkerCode.class
							.getResourceAsStream("AwsCredentials.properties"));
			setEc2(new AmazonEC2Client(credentials));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws Exception {
		Logger l = new Logger();
		l.logging("***************************************************************************");
		l.logging("***************************************************************************");
		l.logging("Luna Worker Code");
		l.logging("Developed by Eduardo Hernandez Marquina, Hector Veiga and Gerardo Travesedo");
		l.logging("***************************************************************************");
		l.logging("");

		// Getting the Data (rowID and receipHandle) form inside the file
		BufferedReader bf = new BufferedReader(new FileReader("data.txt"));
		String data = bf.readLine();
		bf.close();
		String[] parts = data.split(",");
		int rowID = Integer.parseInt(parts[0]);
		String receiptHandle = parts[1];

		// Launching the threads
		InstanceManager instanceManager = new InstanceManager(getEc2()); 
		Message msg = new Message();
		new Renderer(rowID, receiptHandle, msg, instanceManager);
		new ClientWorkerThread(rowID, receiptHandle, msg, getServerName(),
				getPort(), instanceManager);
		l.logging("Launched rendereing and communication threads ");

		/*
		 * In case of malfunction (deadlock or other kinds of errors) where the
		 * renderer thread can not terminate the EC2-Instance, this thread will
		 * do the job. The Renderer will have 11 hours that is also the
		 * visibility timeout of the SQS message.
		 */
		/*
		 * If everything goes well, this below piece of code would never have to
		 * be executed
		 */
		Thread.sleep(11 * 60 * 60 * 1000);
		
		// Killing the instance and sending error logs
		msg.putResource(l.logging("An Error has been produced:"));
		msg.putResource(l
				.logging("Something was wrong with the execution of the Renderer thread or/and Communication Thread"));
		msg.putResource(l
				.logging("The main thread had to terminate the worker instance with the rowID="
						+ rowID));
		// Sleep 3 seconds to give some time to the CliantWorkerThread to send
		// those logs
		Thread.sleep(3 * 1000);
		instanceManager.killmePlease();
	}

	public static AmazonEC2 getEc2() {
		return ec2;
	}

	public static void setEc2(AmazonEC2 ec2) {
		WorkerCode.ec2 = ec2;
	}

	public static String getServerName() {
		return serverName;
	}

	public static int getPort() {
		return port;
	}

}
