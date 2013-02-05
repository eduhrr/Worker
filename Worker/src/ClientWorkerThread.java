import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.Random;


public class ClientWorkerThread implements Runnable {
	private String dataToSend;
	
	public ClientWorkerThread(String data) {
		super();
		this.setDataToSend(data);
		mandarMens.logging("Retrieved data in thread" + getDataToSend() );
	}

	@Override
	public void run() {
		String serverName = "54.243.226.19";//TODO: change to EC2 private IP
		int port = 6060;
		DataInputStream input;
		DataOutputStream output;
		
		try {
			Socket client = new Socket(serverName, port);
			client.setSoTimeout(600000);  //TODO: 10 minutes ->control if Master is not using it
		    //client.setSoTimeout(36000000);  //10 hours ->control if Master is not using it
			input = new DataInputStream(client.getInputStream());
			output = new DataOutputStream(client.getOutputStream());
			
			//send data to the master
			output.writeUTF(this.getDataToSend());
			mandarMens.logging("Sent data in thread" + getDataToSend() );
			
			//listen de rowID
			//int rowID = input.readInt();
			//System.out.println("rowID="+rowID);
			
			//Doing the stuff
			Thread.sleep(3*60*100); //3 minutes
			
			//send the result
			Random num = new Random(port);
			if (num.nextBoolean()){
				output.writeUTF("DONE");
			}else{
				output.writeUTF("BAD RESULT");
			}
			
			
			output.close();
	        input.close();
			client.close();
			// Kill instance
			mandarMens.killmePlease();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public String getDataToSend() {
		return dataToSend;
	}

	public void setDataToSend(String dataToSend) {
		this.dataToSend = dataToSend;
	}

//	public static void main(String[] args) {
//			Runnable newClient = new ClientWorkerThread(); 
//	        Thread t = new Thread(newClient);
//	        t.start();  
//	}
	
}

