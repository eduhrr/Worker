import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.Random;

public class ClientWorkerThread implements Runnable {
	private String dataToSend;
	private Message messageObject;

	public ClientWorkerThread(String data, Message messageObjet) {
		super();
		this.setDataToSend(data);
		setMessageObject(messageObjet);
		mandarMens.logging("Retrieved data in thread " + getDataToSend()); // TODO
																			// ??
	}

	@Override
	public void run() {
		String serverName = "54.243.226.19";// TODO: change to EC2 private IP
											// Hard CODE
		int port = 6060;
		DataInputStream input;
		DataOutputStream output;

		try {
			// mandarMens.logging("here 1");
			Socket client = new Socket(serverName, port);
			// mandarMens.logging("here 2");
			client.setSoTimeout(600000); // TODO: 10 minutes ->control if Master
											// is not using it
			// client.setSoTimeout(36000000); //10 hours ->control if Master is
			// not using it
			// mandarMens.logging("here 3");
			input = new DataInputStream(client.getInputStream());
			output = new DataOutputStream(client.getOutputStream());

			// send data to LunaCore
			output.writeUTF(this.getDataToSend());
			mandarMens.logging("Sent data in thread" + getDataToSend());

			// listen de rowID fomr the LunaCore
			// int rowID = input.readInt();
			// System.out.println("rowID="+rowID);

			// The Sending worker's Status while
			String msg = "";
			do {
				// the next statement will sleep the thread until a new Shared
				// resource will be ready to comsume
				msg = getMessageObject().getResource();
				output.writeUTF(msg);
			} while (!msg.equals("The rendering has been finished"));

			// Thread.sleep(3*60*100); //3 minutes

			// //send the result
			// Random num = new Random(port);
			// if (num.nextBoolean()){
			// output.writeUTF("DONE");
			// }else{
			// output.writeUTF("BAD RESULT");
			// }

			output.close();
			input.close();
			client.close();
			// Kill instance
			mandarMens.killmePlease();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public String getDataToSend() {
		return dataToSend;
	}

	public void setDataToSend(String dataToSend) {
		this.dataToSend = dataToSend;
	}

	public Message getMessageObject() {
		return messageObject;
	}

	public void setMessageObject(Message messageObject) {
		this.messageObject = messageObject;
	}

	// public static void main(String[] args) {
	// Runnable newClient = new ClientWorkerThread();
	// Thread t = new Thread(newClient);
	// t.start();
	// }

}
