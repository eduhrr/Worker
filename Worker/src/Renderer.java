/**
 * 
 */
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.zip.ZipFile;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.StorageClass;

/**
 * Responsible class for the rendering job
 * 
 * @author Eduardo Hernandez Marquina
 * @author Hector Veiga
 * @author Gerardo Travesedo
 * 
 */
public class Renderer implements Runnable {

	private int rowID;
	private String receiptHandle;
	private Message MessageObject;
	private InstanceManager instanceManager;
	private static AmazonS3 s3;

	public Renderer(int rowID, String receiptHandle, Message msg,
			InstanceManager instanceManager, AmazonS3 s3) {
		super();
		setReceiptHandle(receiptHandle);
		setRowID(rowID);
		setMessageObject(msg);
		setInstanceManager(instanceManager);
		setS3(s3);
		new Thread(this, "Renderer").start();
	}

	@Override
	public void run() {
		Logger l = new Logger();

		//Timer ??
		Date time1 = new Date();
		long epoch1 = (long) System.currentTimeMillis() / 1000;


		try { 
			// Testing rendering process
			//Process p = Runtime.getRuntime().exec("sleep 60");
			
			// DriverManager.registerDriver(new com.mysql.jdbc.Driver());
			String url = "jdbc:mysql://iitLuna.tk:3306/luna";
			// String url = "jdbc:mysql://localhost:3306/mysql";
			String username = "java";
			String password = "edu";
			Connection connection = null;
			
			//DB test
//			try {
//				System.out.println("Connecting database...");
//				connection = DriverManager.getConnection(url, username, password);
//				System.out.println("Database connected!");
//
//				Statement s = connection.createStatement();
//				ResultSet rs = s
//						.executeQuery("SELECT * FROM Credentials WHERE name = 'edu'");
//				while (rs.next()) {
//					l.logging(rs.getString(1));
//					l.logging(rs.getString(2));
//				}
//			} catch (SQLException e) {
//				throw new RuntimeException("Cannot connect the database!", e);
//			} finally {
//				System.out.println("Closing the connection.");
//				if (connection != null)
//					try {
//						connection.close();
//					} catch (SQLException ignore) {
//					}
//			}
			
			
			
			
			//Starts Hector Code  -->factorizable a clase con mazo de atributos
			String userId = null;  //??
			String className = null;
			String classDate = null;
			String format = null;
			String S3BucketOriginal = null;
			String S3BucketFinished = null;
			String S3KeyOriginal = null;
			String S3KeyFinished = null;
			String part = null;
			boolean autorender = false;
			String output = "";
			l.logging("Trying to fetch information from database...");
			// Getting info from the DB
			try {
				// DriverManager.registerDriver(new com.mysql.jdbc.Driver());
				//
				connection = DriverManager.getConnection(
						url, username, password);
				Statement s = connection.createStatement();
				//"SELECT * FROM Credentials WHERE name = 'edu'"
				
				ResultSet rs = s.executeQuery("SELECT * FROM requests WHERE id = '"
						+ getRowID() + "'");
				//TODO: PARACE QUE NO COGE NADA DE LA BASE DE DATOS
				while (rs.next()) {
					userId = rs.getString(2);
					getMessageObject().putResource(l.logging("userId= " + userId));
					className = rs.getString(3);
					getMessageObject().putResource(l.logging("className= "+className));
					classDate = rs.getString(4);
					getMessageObject().putResource(l.logging("classDate= "+classDate));
					format = rs.getString(6);
					getMessageObject().putResource(l.logging("format= "+format)); 
					S3BucketOriginal = rs.getString(7);
					getMessageObject().putResource(l.logging("S3BucketOriginal= "+S3BucketOriginal));
					S3BucketFinished = rs.getString(8);
					getMessageObject().putResource(l.logging("S3BucketFinished= "+S3BucketFinished));
					S3KeyOriginal = rs.getString(9);
					getMessageObject().putResource(l.logging("S3KeyOriginal= "+S3KeyOriginal));
					S3KeyFinished = rs.getString(10);
					getMessageObject().putResource(l.logging("S3KeyFinished= "+S3KeyFinished));
					if (Integer.parseInt(rs.getString(12)) == 1)
						autorender = true;
					part = rs.getString(13);
				}
				rs.close();
				s.close();
				connection.close();
			} catch (Exception e) {
				getMessageObject().putResource(l.logging("Error retrieving data from table 'requests'. Existing..."));
				e.printStackTrace();
				getInstanceManager().killmePlease();
			}

			if (S3KeyOriginal == null || S3KeyOriginal.equals("")) {
				getInstanceManager().killmePlease();
			}
			getMessageObject().putResource(l.logging("Data retrieved from table 'requests' successfully"));

			// Download from S3
			getMessageObject().putResource(l.logging("Trying to download '" + S3KeyOriginal
					+ "' from luna-videos-before S3 bucket..."));
			GetObjectRequest S3Original = new GetObjectRequest(S3BucketOriginal,
					S3KeyOriginal);
			S3Object originalObject = s3.getObject(S3Original);

			InputStream reader = new BufferedInputStream(
					originalObject.getObjectContent());
			File file = new File(S3KeyOriginal);
			OutputStream writer = new BufferedOutputStream(new FileOutputStream(
					file));

			int read = -1;
			while ((read = reader.read()) != -1) {
				writer.write(read);
			}

			writer.flush();
			writer.close();
			reader.close();

			getMessageObject().putResource(l.logging("File retrieved succesfully."));

			boolean isZip = false;

			// It works just with .zip files
			getMessageObject().putResource(l.logging("Checking if it is a Zip File..."));
			String[] nameExt = S3KeyOriginal.split("\\.");
			if (nameExt[nameExt.length - 1].equals("zip")
					|| nameExt[nameExt.length - 1].equals("ZIP")) {
				ZipFile sourceZipFile = new ZipFile(S3KeyOriginal);
				getMessageObject().putResource(l.logging("Zip File found. Decompressing and removing .zip file."));
				String unzip_filename = sourceZipFile.entries().nextElement()
						.getName();
				Process p = Runtime.getRuntime()
						.exec("sudo unzip " + S3KeyOriginal);
				//
				waitingProcessWhile(p, l);
				//p.waitFor();
				Process p2 = Runtime.getRuntime().exec("sudo rm " + S3KeyOriginal);
				waitingProcessWhile(p2, l);
				//p2.waitFor();
				S3KeyOriginal = unzip_filename;
				isZip = true;
			}

			getMessageObject().putResource(l.logging("Setting up script to convert:"));

			// Conversion script
			nameExt = S3KeyOriginal.split("\\.");
			boolean isTS = false;
			if (nameExt[nameExt.length - 1].equals("TS")
					|| nameExt[nameExt.length - 1].equals("ts")) {
				isTS = true;
			}
			int extensionSize = nameExt[nameExt.length - 1].length();
			String fileName = S3KeyOriginal.substring(0, S3KeyOriginal.length() - 1
					- extensionSize);

			if (isZip) {
				int randNum = (int) (Math.random() * 1000000000);
				fileName = "LV" + randNum + "_" + fileName;
			}

			String command = "";
			String finishedName = "";
			if (format.equals("ipad")) {
				finishedName = fileName + "_ipad.mp4";
				getMessageObject().putResource(l.logging("Converting file to iPad format. Output filename: "
						+ finishedName));
				command = "sudo HandBrakeCLI -i " + S3KeyOriginal + " -o "
						+ finishedName
						+ " -e x264 -q 32 -B 128 -w 800 --loose-anamorphic -O";
			} else if (format.equals("mp4")) {
				finishedName = fileName + "_fullsize.mp4";
				getMessageObject().putResource(l.logging("Converting file to Fullsize format. Output filename: "
						+ finishedName));
				command = "sudo HandBrakeCLI -v -i " + S3KeyOriginal + " -o "
						+ finishedName + " -e x264 -q 32 -B 128 -O";
			} else if (format.equals("iphone")) {
				finishedName = fileName + "_iphone.mp4";
				getMessageObject().putResource(l.logging("Converting file to iPhone format. Output filename: "
						+ finishedName));
				command = "sudo HandBrakeCLI -i " + S3KeyOriginal + " -o "
						+ finishedName
						+ " -e x264 -q 32 -B 128 -w 640 --loose-anamorphic -O";
			} else if (format.equals("mp3")) {
				getMessageObject().putResource(l.logging("Extracting audio from file. Output filename: "
						+ finishedName));
				finishedName = fileName + "_audio.mp3";
				command = "sudo ffmpeg -y -i " + S3KeyOriginal
						+ " -vn -ab 96k -ar 44100 -f mp3 " + finishedName;
			}

			S3KeyFinished = finishedName;
			getMessageObject().putResource(l.logging("Command to convert: " + command));
			getMessageObject().putResource(l.logging("Starting to convert"));
			StringBuilder outputProc = new StringBuilder();

			// Percentage Test variables
			int totalSecs = 0;
			int linesPerUpdate = 35;
			int linesCount = 0;

			//
			updateStatus("converting", url, username, password, l);

			try {
				String line = "";
				Process p = Runtime.getRuntime().exec(command);
				BufferedReader bri = new BufferedReader(new InputStreamReader(
						p.getErrorStream()));
				while ((line = bri.readLine()) != null) {
					outputProc.append(line + "\n");
					l.logging(line);
					if ((!format.equals("mp3")) && line.contains("work result = 0"))
						p.destroy();

					if (!isTS || (format.equals("mp3"))) {
						// Percetage done (FFMPEG ONLY)
						if (line.contains("Duration")) {
							try {
								totalSecs = giveMeSeconds(line, "Duration: ", ",");
							} catch (Exception e) {
								e.printStackTrace();
							}
						}
					}
					if ((!isTS && linesCount == linesPerUpdate)
							|| (format.equals("mp3") && linesCount == linesPerUpdate)) {
						if ((line.contains("frame=") && line.contains("fps=") && line
								.contains("time="))
								|| (line.contains("time=") && line
										.contains("size="))) {
							try {
								int elapsedSecs = giveMeSeconds(line, "time=", " ");
								double percentage = ((double) elapsedSecs / (double) totalSecs) * 100;
								//
								updateStatus(Double.toString(percentage).substring(
										0, 5), url, username, password,l);
							} catch (Exception e) {
							}
						}
						linesCount = 0;
					}
					linesCount++;
				}
				bri.close();
				getMessageObject().putResource(l.logging("Waiting for process to finish"));
	//p.waitFor();
				// Sending the status every 30 minutes to keep alive the
				// LunaCore Communication thread
				int i = 0;
				int hours, min;
				while (!processIsTerminated(p)) {
					hours = (i * 30) % 60;
					min = i * 30 - hours * 60;
					getMessageObject().putResource(
							l.logging("Renderer is working, elapsed time: " + hours
									+ " hours and " + min + " minutes"));
					i += 1;
					Thread.sleep(60 * 1000);
				}
				l.logging("File converted successfully");
				
			} catch (Exception e) {
				e.printStackTrace();
			}

			getMessageObject().putResource(l.logging("Trying to upload it to S3..."));
			// Upload to S3 (Check multipart-uploading to upload faster)
			File finishedFile = new File(finishedName);
			PutObjectRequest por = new PutObjectRequest(S3BucketFinished,
					finishedName, finishedFile);
			por.setStorageClass(StorageClass.ReducedRedundancy);
			// por.setCannedAcl(CannedAccessControlList.PublicRead);
			s3.putObject(por);
			getMessageObject().putResource(l.logging("File uploaded to S3 successfully"));

			// Upload to Walrus would be here!

			// Update Database Status
			getMessageObject().putResource(l.logging("Updating databases...")); //
			try {
				connection = DriverManager.getConnection( //
						url, username, password);
				Statement s = connection.createStatement();
				String query1 = "INSERT INTO videos (class,classDate,format,S3BucketFinished,S3KeyFinished,datefinished,part) VALUES "
						+ "('"
						+ className
						+ "',"
						+ classDate
						+ ",'"
						+ format
						+ "','"
						+ S3BucketFinished
						+ "','"
						+ S3KeyFinished
						+ "',UNIX_TIMESTAMP(now()),'" + part + "')";
				s.executeUpdate(query1);
				s.close();
				String query2 = "DELETE FROM requests WHERE id='" + getRowID() + "'";
				Statement s2 = connection.createStatement();
				s2.executeUpdate(query2);
				s2.close();
				String query3 = "SELECT COUNT(*) FROM videos WHERE (format='mp3' OR format='mp4') AND part='"
						+ part
						+ "' AND class='"
						+ className
						+ "' AND classDate='"
						+ classDate + "'";
				Statement s_count = connection.createStatement();
				ResultSet rs = s_count.executeQuery(query3);
				while (rs.next()) {
					if (rs.getInt("COUNT(*)") == 2) {
						DeleteObjectRequest del = new DeleteObjectRequest(
								S3BucketOriginal, S3KeyOriginal);
						s3.deleteObject(del);
						String query4 = "DELETE FROM videos WHERE format='original' AND part='"
								+ part
								+ "' AND class='"
								+ className
								+ "' AND classDate='" + classDate + "'";
						Statement s_del = connection.createStatement();
						s_del.executeUpdate(query4);
						s_del.close();
					}
				}
				s_count.close();
				connection.close();
				getMessageObject().putResource(l.logging("Databases updated: request row deleted and video row created."));
			} catch (Exception e) {
				getMessageObject().putResource(l.logging("Exception catch! Something wrong updating Database"));
				getMessageObject().putResource(l.logging(e.toString()));
			}

			// SES-SNS
			Date time2 = new Date();
			long epoch2 = (long) System.currentTimeMillis() / 1000;
			long elapsedTime = epoch2 - epoch1;

			StringBuilder sb = new StringBuilder();
			sb.append("File is located in: https://" + S3BucketFinished
					+ ".s3.amazonaws.com/" + finishedName + " \n\n\n");
			sb.append("Debug information:\n");
			sb.append("Start time: " + time1 + "\n");
			sb.append("Finish time: " + time2 + "\n\n");
			sb.append("Output:\n");
			sb.append(outputProc.toString());
			output = sb.toString();

			/*
			 * Right now, everything is autorender. String subject = "";
			 * 
			 * if(format.equals("mp3")) { subject = "Luna: Your audio is ready!"; }
			 * else { subject = "Luna: Your video is ready!"; } if(!autorender) {
			 * logging("Sending email to user..."); Jmailserver mail = new
			 * Jmailserver("gmail.com", "noreplylunaproject", "LunaMoonlight");
			 * mail.sendMessage(userId, subject,
			 * "You can watch your media using our website: https://luna.sat.iit.edu"
			 * , "text/plain"); }
			 */

			// Logs for debugging
			getMessageObject().putResource(l.logging("Information for debugging:"));
			String instanceType = getInstanceManager().getInstanceType();
			try {
				String origType = nameExt[nameExt.length - 1];
				getMessageObject().putResource(l.logging("ORIGTYPE = " + origType));
				getMessageObject().putResource(l.logging("INSTANCETYPE = " + instanceType));
				int origSize = (int) file.length();
				getMessageObject().putResource(l.logging("OrigSize = " + origSize));
				int finSize = (int) finishedFile.length();
				getMessageObject().putResource(l.logging("FinSize = " + finSize));
				String output2 = output.replace("'", " ");
				String output3 = output2.replace("\"", " ");
				getMessageObject().putResource(l.logging("Output = " + output3));

				connection = DriverManager.getConnection(  //
						url, username, password);
				Statement s = connection.createStatement();
				String query = "INSERT INTO logs (startTime,finishTime,originalType,finishedType,instanceSize,originalFileSize,finalFileSize,output) VALUES "
						+ "("
						+ epoch1
						+ ","
						+ epoch2
						+ ",'"
						+ origType
						+ "','"
						+ format
						+ "','"
						+ instanceType
						+ "',"
						+ origSize
						+ ","
						+ finSize + ",'" + output3 + "')";

				s.executeUpdate(query);
				s.close();
				connection.close();
			} catch (Exception e) {
				getMessageObject().putResource(l.logging("Exception catch!!!! Something wrong happened creating logs"));
				getMessageObject().putResource(l.logging(e.toString()));
			}

			// Updating parameters for statistics
			if (autorender) {
				updateParameter("autoConversions", 1, url, username, password, l);
			} else {
				updateParameter("onDemandConversions", 1, url, username, password, l);
			}
			if (instanceType.toLowerCase().contains("micro")) {
				updateParameter("elapsedTimeMicro", elapsedTime, url, username, password, l);
			} else {
				updateParameter("elapsedTimeMedium", elapsedTime, url, username, password, l);
			}

			// Done
			getMessageObject().putResource(l.logging("Everything done. Autokilling!"));

			// Kill instance
			//killmePlease();


			getMessageObject().putResource("The rendering has been finished");
			} catch (FileNotFoundException e1) {
				
				e1.printStackTrace();
			} catch (IOException e1) {
				
				e1.printStackTrace();
			} catch (InterruptedException e) {
				
				e.printStackTrace();
			} 
		
	}

	// To know when a external process is terminated
	private boolean processIsTerminated(Process p) {
		try {
			p.exitValue();
		} catch (IllegalThreadStateException itse) {
			return false;
		}
		return true;
	}
	
	// Sending the status every 30 minutes to keep alive the
	// LunaCore Communication thread
	private void waitingProcessWhile (Process p, Logger l) throws InterruptedException{
		int i = 0;
		int hours, min;
		while (!processIsTerminated(p)) {
			hours = (i * 30) % 60;
			min = i * 30 - hours * 60;
			getMessageObject().putResource(
					l.logging("Renderer is working, elapsed time: " + hours
							+ " hours and " + min + " minutes"));
			i += 1;
			Thread.sleep(60 * 1000);
		}
	}

	private String substringBetween(String str, String open, String close) {
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


	private int giveMeSeconds(String line, String del1, String del2) {
		String durVid = "";
		String[] durVidPieces;

		durVid = substringBetween(line, del1, del2);
		durVidPieces = durVid.split(":");
		int secs = (Integer.parseInt(durVidPieces[0]) * 3600)
				+ (Integer.parseInt(durVidPieces[1]) * 60)
				+ Integer.parseInt(durVidPieces[2].substring(0, 2));
		return secs;
	}

	private void updateStatus(String percentage, String url,
			String username, String password, Logger l) {
		// Update Database Status
		try {
			Connection connec = DriverManager.getConnection(
					url, username, password);
			Statement s = connec.createStatement();
			String query1 = "UPDATE requests SET status='" + percentage
					+ "' WHERE id='" + getRowID() + "'";
			s.executeUpdate(query1);
			s.close();
			connec.close();
		} catch (Exception e) {
			l.logging("Exception catch!!!! Something wrong updating status of a request");
			l.logging(e.toString());
		}
	}

	 private void updateParameter(String parameter, long quantity, String url,
				String username, String password, Logger l) {
	 l.logging("Updating parameter '" + parameter + "' by " + quantity);
	
	 // Update Database Parameter
	 try {
	 Connection connec = DriverManager.getConnection(
	 url, username, password);
	 Statement s = connec.createStatement();
	 String query1 = "UPDATE parameters SET value=value+"
	 + String.valueOf(quantity) + " WHERE parameter='"
	 + parameter + "'";
	 s.executeUpdate(query1);
	 s.close();
	 connec.close();
	 } catch (Exception e) {
	 l.logging("Exception catch!!!! Something wrong updating status of a request");
	 l.logging(e.toString());
	 }
	 }

	public int getRowID() {
		return rowID;
	}

	public void setRowID(int rowID2) {
		this.rowID = rowID2;
	}

	public String getReceiptHandle() {
		return receiptHandle;
	}

	public void setReceiptHandle(String receiptHandle) {
		this.receiptHandle = receiptHandle;
	}

	public Message getMessageObject() {
		return MessageObject;
	}

	public void setMessageObject(Message messageObject) {
		MessageObject = messageObject;
	}

	public InstanceManager getInstanceManager() {
		return instanceManager;
	}

	public void setInstanceManager(InstanceManager instanceManager) {
		this.instanceManager = instanceManager;
	}

	public static AmazonS3 getS3() {
		return s3;
	}

	public static void setS3(AmazonS3 s3) {
		Renderer.s3 = s3;
	}

}
