import java.util.ArrayList;
import java.util.List;
import java.util.Date;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.sql.*;
import java.util.zip.ZipFile;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.StorageClass;

/**
 * LunaWorkerApp
 */
public class AwsConsoleApp {
	static AmazonEC2 ec2;
	static AmazonS3 s3;
	static String rowId;

	private static void init() throws Exception {
		AWSCredentials credentials = new PropertiesCredentials(
				AwsConsoleApp.class
						.getResourceAsStream("AwsCredentials.properties"));
		ec2 = new AmazonEC2Client(credentials);
		s3 = new AmazonS3Client(credentials);
	}

	public static void main(String[] args) throws Exception {
		logging("Luna Worker Script");
		logging("Developed by Hector Veiga and Gerardo Travesedo");
		logging("");
		init();
		Date time1 = new Date();
		long epoch1 = (long) System.currentTimeMillis() / 1000;

		rowId = System.getenv("rowId");
		logging("Id of row of request retrieved: " + rowId);

		String userId = null;
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
		logging("Trying to fetch information from database...");
		// Getting info from the DB
		try {
			// DriverManager.registerDriver(new com.mysql.jdbc.Driver());
			Connection connec = DriverManager.getConnection(
					"jdbc:mysql://64.131.110.162/luna", "Europa", "a23d578");
			Statement s = connec.createStatement();
			ResultSet rs = s.executeQuery("SELECT * FROM requests WHERE id='"
					+ rowId + "'");
			while (rs.next()) {
				userId = rs.getString(2);
				className = rs.getString(3);
				classDate = rs.getString(4);
				format = rs.getString(6);
				S3BucketOriginal = rs.getString(7);
				S3BucketFinished = rs.getString(8);
				S3KeyOriginal = rs.getString(9);
				S3KeyFinished = rs.getString(10);
				if (Integer.parseInt(rs.getString(12)) == 1)
					autorender = true;
				part = rs.getString(13);
			}
			rs.close();
			s.close();
			connec.close();
		} catch (Exception e) {
			logging("Error retrieving data from table 'requests'. Existing...");
			e.printStackTrace();
			killmePlease();
		}

		if (S3KeyOriginal == null || S3KeyOriginal.equals("")) {
			killmePlease();
		}
		logging("Data retrieved from table 'requests' successfully");

		// Start Timer to kill instance if it does not finish
		logging("Setting timer (10h) to autokill instance in case it freezes");
		TimerKill timer = new TimerKill(36000000); // 10h
		timer.start();

		// Download from S3
		logging("Trying to download '" + S3KeyOriginal
				+ "' from luna-videos-before S3 bucket...");
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

		logging("File retrieved succesfully.");

		boolean isZip = false;

		// It works just with .zip files
		logging("Checking if it is a Zip File...");
		String[] nameExt = S3KeyOriginal.split("\\.");
		if (nameExt[nameExt.length - 1].equals("zip")
				|| nameExt[nameExt.length - 1].equals("ZIP")) {
			ZipFile sourceZipFile = new ZipFile(S3KeyOriginal);
			logging("Zip File found. Decompressing and removing .zip file.");
			String unzip_filename = sourceZipFile.entries().nextElement()
					.getName();
			Process p = Runtime.getRuntime()
					.exec("sudo unzip " + S3KeyOriginal);
			p.waitFor();
			Process p2 = Runtime.getRuntime().exec("sudo rm " + S3KeyOriginal);
			p2.waitFor();
			S3KeyOriginal = unzip_filename;
			isZip = true;
		}

		logging("Setting up script to convert:");

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
			logging("Converting file to iPad format. Output filename: "
					+ finishedName);
			command = "sudo HandBrakeCLI -i " + S3KeyOriginal + " -o "
					+ finishedName
					+ " -e x264 -q 32 -B 128 -w 800 --loose-anamorphic -O";
		} else if (format.equals("mp4")) {
			finishedName = fileName + "_fullsize.mp4";
			logging("Converting file to Fullsize format. Output filename: "
					+ finishedName);
			command = "sudo HandBrakeCLI -v -i " + S3KeyOriginal + " -o "
					+ finishedName + " -e x264 -q 32 -B 128 -O";
		} else if (format.equals("iphone")) {
			finishedName = fileName + "_iphone.mp4";
			logging("Converting file to iPhone format. Output filename: "
					+ finishedName);
			command = "sudo HandBrakeCLI -i " + S3KeyOriginal + " -o "
					+ finishedName
					+ " -e x264 -q 32 -B 128 -w 640 --loose-anamorphic -O";
		} else if (format.equals("mp3")) {
			logging("Extracting audio from file. Output filename: "
					+ finishedName);
			finishedName = fileName + "_audio.mp3";
			command = "sudo ffmpeg -y -i " + S3KeyOriginal
					+ " -vn -ab 96k -ar 44100 -f mp3 " + finishedName;
		}

		S3KeyFinished = finishedName;
		logging("Command to convert: " + command);
		logging("Starting to convert");
		StringBuilder outputProc = new StringBuilder();

		// Percentage Test variables
		int totalSecs = 0;
		int linesPerUpdate = 35;
		int linesCount = 0;

		updateStatus("converting");

		try {
			String line = "";
			Process p = Runtime.getRuntime().exec(command);
			BufferedReader bri = new BufferedReader(new InputStreamReader(
					p.getErrorStream()));
			while ((line = bri.readLine()) != null) {
				outputProc.append(line + "\n");
				logging(line);
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
							updateStatus(Double.toString(percentage).substring(
									0, 5));
						} catch (Exception e) {
						}
					}
					linesCount = 0;
				}
				linesCount++;
			}
			bri.close();
			logging("Waiting for process to finish");
			p.waitFor();
			logging("File converted successfully");
		} catch (Exception e) {
			e.printStackTrace();
		}

		logging("Trying to upload it to S3...");
		// Upload to S3 (Check multipart-uploading to upload faster)
		File finishedFile = new File(finishedName);
		PutObjectRequest por = new PutObjectRequest(S3BucketFinished,
				finishedName, finishedFile);
		por.setStorageClass(StorageClass.ReducedRedundancy);
		// por.setCannedAcl(CannedAccessControlList.PublicRead);
		s3.putObject(por);
		logging("File uploaded to S3 successfully");

		// Upload to Walrus would be here!

		// Update Database Status
		logging("Updating databases...");
		try {
			Connection connec = DriverManager.getConnection(
					"jdbc:mysql://64.131.110.162/luna", "Europa", "a23d578");
			Statement s = connec.createStatement();
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
			String query2 = "DELETE FROM requests WHERE id='" + rowId + "'";
			Statement s2 = connec.createStatement();
			s2.executeUpdate(query2);
			s2.close();
			String query3 = "SELECT COUNT(*) FROM videos WHERE (format='mp3' OR format='mp4') AND part='"
					+ part
					+ "' AND class='"
					+ className
					+ "' AND classDate='"
					+ classDate + "'";
			Statement s_count = connec.createStatement();
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
					Statement s_del = connec.createStatement();
					s_del.executeUpdate(query4);
					s_del.close();
				}
			}
			s_count.close();
			connec.close();
			logging("Databases updated: request row deleted and video row created.");
		} catch (Exception e) {
			logging("Exception catch! Something wrong updating Database");
			logging(e.toString());
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
		logging("Information for debugging:");
		String instanceType = getInstanceType();
		try {
			String origType = nameExt[nameExt.length - 1];
			logging("ORIGTYPE = " + origType);
			logging("INSTANCETYPE = " + instanceType);
			int origSize = (int) file.length();
			logging("OrigSize = " + origSize);
			int finSize = (int) finishedFile.length();
			logging("FinSize = " + finSize);
			String output2 = output.replace("'", " ");
			String output3 = output2.replace("\"", " ");
			logging("Output = " + output3);

			Connection connec = DriverManager.getConnection(
					"jdbc:mysql://64.131.110.162/luna", "Europa", "a23d578");
			Statement s = connec.createStatement();
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
			connec.close();
		} catch (Exception e) {
			logging("Exception catch!!!! Something wrong happened creating logs");
			logging(e.toString());
		}

		// Upadating parameters for statistics
		if (autorender) {
			updateParameter("autoConversions", 1);
		} else {
			updateParameter("onDemandConversions", 1);
		}
		if (instanceType.toLowerCase().contains("micro")) {
			updateParameter("elapsedTimeMicro", elapsedTime);
		} else {
			updateParameter("elapsedTimeMedium", elapsedTime);
		}

		// Done
		logging("Everything done. Autokilling!");

		// Kill instance
		killmePlease();

	}

	public static void killmePlease() {
		// Kill myself
		logging("Terminating instance");
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
		ec2.terminateInstances(terminateRequest);
	}

	private static String getInstanceType() {
		String instanceType = "";
		try {
			Process p = Runtime
					.getRuntime()
					.exec("sudo wget -q -O - http://169.254.169.254/latest/meta-data/instance-type");
			p.waitFor();
			BufferedReader br = new BufferedReader(new InputStreamReader(
					p.getInputStream()));
			instanceType = br.readLine();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return instanceType;
	}

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

	private static void updateStatus(String percentage) {
		// Update Database Status
		try {
			Connection connec = DriverManager.getConnection(
					"jdbc:mysql://64.131.110.162/luna", "Europa", "a23d578");
			Statement s = connec.createStatement();
			String query1 = "UPDATE requests SET status='" + percentage
					+ "' WHERE id='" + rowId + "'";
			s.executeUpdate(query1);
			s.close();
			connec.close();
		} catch (Exception e) {
			logging("Exception catch!!!! Something wrong updating status of a request");
			logging(e.toString());
		}
	}

	private static void updateParameter(String parameter, long quantity) {
		logging("Updating parameter '" + parameter + "' by " + quantity);

		// Update Database Parameter
		try {
			Connection connec = DriverManager.getConnection(
					"jdbc:mysql://64.131.110.162/luna", "Europa", "a23d578");
			Statement s = connec.createStatement();
			String query1 = "UPDATE parameters SET value=value+"
					+ String.valueOf(quantity) + " WHERE parameter='"
					+ parameter + "'";
			s.executeUpdate(query1);
			s.close();
			connec.close();
		} catch (Exception e) {
			logging("Exception catch!!!! Something wrong updating status of a request");
			logging(e.toString());
		}
	}

	private static void logging(String lineToLog) {
		Date time = new Date();
		String line = "[" + time.toString() + "] " + lineToLog;
		System.out.println(line);
	}
}
