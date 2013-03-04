import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.DeleteObjectRequest;

/**
 * Class Responsible for the Data Base work
 * 
 * @author Eduardo Hernandez Marquina
 * @author Hector Veiga
 * @author Gerardo Travesedo
 * 
 */
public class DBManager {

	private static String url; // The String
								// "jdbc:mysql://ServerName:Port/DBName"
	private static String username;
	private static String password;
	private Connection connection;

	public DBManager(String serverNameDB, int portDB, String nameDB,
			String userNanme, String password) {
		setUrl("jdbc:mysql://" + serverNameDB + ":" + portDB + "/" + nameDB);
		System.out.println(getUrl());
		setUsername(userNanme);
		setPassword(password);
	}

	/**
	 * Gets the info of a Video given a rowID
	 * 
	 * @param rowID
	 * @param ms
	 * @param instanceManager
	 * @param l
	 * @return
	 */
	public VideoData getInfo(int rowID, Message ms,
			InstanceManager instanceManager, Logger l) {

		VideoData vData = new VideoData();
		l.logging("Trying to fetch information from database...");

		// Getting info from the DB
		try {
			startConnection();
			Statement s = getConnection().createStatement();
			String query = "SELECT * FROM requests WHERE id = '" + rowID + "'";
			ResultSet rs = s.executeQuery(query);
			while (rs.next()) {
				vData.setUserId(rs.getString(2));
				vData.setClassName(rs.getString(3));
				vData.setClassDate(rs.getString(4));
				vData.setFormat(rs.getString(6));
				vData.setS3BucketOriginal(rs.getString(7).toLowerCase());
				vData.setS3BucketFinished(rs.getString(8).toLowerCase());
				vData.setS3KeyOriginal(rs.getString(9));
				vData.setS3KeyFinished(rs.getString(10));
				vData.setAutorender(rs.getString(12));
				vData.setPart(rs.getString(13));
			}
			rs.close();
			s.close();
		} catch (Exception e) {
			ms.putResource(l
					.logging("Error retrieving data from table 'requests'. Existing..."));
			e.printStackTrace();
			instanceManager.killmePlease();
		} finally {
			if (getConnection() != null)
				try {
					getConnection().close();
				} catch (SQLException ignore) {
				}
		}

		return vData;
	}

	/**
	 * 
	 * @param rowID
	 * @param percentage
	 * @param l
	 */
	public void updateStatus(int rowID, String percentage, Logger l) {
		// Update Database Status
		try {
			startConnection();
			Statement s = getConnection().createStatement();
			String query = "UPDATE requests SET status='" + percentage
					+ "' WHERE id='" + rowID + "'";
			s.executeUpdate(query);
			s.close();
		} catch (Exception e) {
			l.logging("Exception catch!!!! Something wrong updating status of a request");
			l.logging(e.toString());
		} finally {
			if (getConnection() != null)
				try {
					getConnection().close();
				} catch (SQLException ignore) {
				}
		}
	}

	/**
	 *  
	 * @param rowID
	 * @param vData
	 * @param l
	 * @param ms
	 * @param s3
	 */
	public void UpdateStatusRenderingIsFinished(int rowID, VideoData vData,
			Logger l, Message ms, AmazonS3 s3) {
		// Update Database Status when rending is finished

		try {
			startConnection();
			Statement s = getConnection().createStatement();

			// Updating in Video table
			String query1 = "INSERT INTO videos (class,classDate,format,S3BucketFinished,S3KeyFinished,datefinished,part) VALUES "
					+ "('"
					+ vData.getClassName()
					+ "',"
					+ vData.getClassDate()
					+ ",'"
					+ vData.getFormat()
					+ "','"
					+ vData.getS3BucketFinished()
					+ "','"
					+ vData.getS3KeyFinished()
					+ "',UNIX_TIMESTAMP(now()),'"
					+ vData.getPart() + "')";
			s.executeUpdate(query1);
			s.close();

			// Updating/deleting the rowID in request Table
			String query2 = "DELETE FROM requests WHERE id='" + rowID + "'";
			Statement s2 = connection.createStatement();
			s2.executeUpdate(query2);
			s2.close();

			// Updating/deleting the rowID in request Table
			String query3 = "SELECT COUNT(*) FROM videos WHERE (format='mp3' OR format='mp4') AND part='"
					+ vData.getPart()
					+ "' AND class='"
					+ vData.getClassName()
					+ "' AND classDate='" + vData.getClassDate() + "'";
			Statement s_count = connection.createStatement();
			ResultSet rs = s_count.executeQuery(query3);
			while (rs.next()) {

				if (rs.getInt("COUNT(*)") == 2) {
					DeleteObjectRequest del = new DeleteObjectRequest(
							vData.getS3BucketOriginal(),
							vData.getS3KeyOriginal());
					s3.deleteObject(del);

					String query4 = "DELETE FROM videos WHERE format='original' AND part='"
							+ vData.getPart()
							+ "' AND class='"
							+ vData.getClassName()
							+ "' AND classDate='"
							+ vData.getClassDate() + "'";
					Statement s_del = connection.createStatement();
					s_del.executeUpdate(query4);
					s_del.close();
				}
			}
			s_count.close();
			ms.putResource(l
					.logging("Databases updated: request row deleted and video row created."));
		} catch (Exception e) {
			ms.putResource(l
					.logging("Exception catch! Something wrong updating Database"));
			ms.putResource(l.logging(e.toString()));
		} finally {
			if (getConnection() != null)
				try {
					getConnection().close();
				} catch (SQLException ignore) {
				}
		}
	}

	public void UpdateStatusDebuggigData(long epoch1, long epoch2,
			String origType, String instanceType, int origSize, int finSize,
			String output3, VideoData vData, Message ms, Logger l) {

		try {
			startConnection();
			Statement s = getConnection().createStatement();
			String query = "INSERT INTO logs (startTime,finishTime,originalType,finishedType,instanceSize,originalFileSize,finalFileSize,output) VALUES "
					+ "("
					+ epoch1
					+ ","
					+ epoch2
					+ ",'"
					+ origType
					+ "','"
					+ vData.getFormat()
					+ "','"
					+ instanceType
					+ "',"
					+ origSize + "," + finSize + ",'" + output3 + "')";

			s.executeUpdate(query);
			s.close();
		} catch (Exception e) {
			ms.putResource(l
					.logging("Exception catch!!!! Something wrong happened creating logs"));
			ms.putResource(l.logging(e.toString()));
		} finally {
			if (getConnection() != null)
				try {
					getConnection().close();
				} catch (SQLException ignore) {
				}
		}
	}

	public void updateParameter(String parameter, long quantity, Logger l) {
		
		l.logging("Updating parameter '" + parameter + "' by " + quantity);
		// Update Database Parameter
		try {
			startConnection();
			Statement s = getConnection().createStatement();
			String query1 = "UPDATE parameters SET value=value+"
					+ String.valueOf(quantity) + " WHERE parameter='"
					+ parameter + "'";
			s.executeUpdate(query1);
			s.close();
		} catch (Exception e) {
			l.logging("Exception catch!!!! Something wrong updating status of a request");
			l.logging(e.toString());
		} finally {
			if (getConnection() != null)
				try {
					getConnection().close();
				} catch (SQLException ignore) {
				}
		}
	}

	public static void main(String[] args) {
		Logger l = new Logger();
		DBManager db = new DBManager("iitLuna.tk", 3306, "luna", "java", "edu");
		// DB test
		// try {
		// System.out.println("Connecting database...");
		// db.startConnection();
		// System.out.println("Database connected!");
		//
		// Statement s = db.getConnection().createStatement();
		// ResultSet rs = s
		// .executeQuery("SELECT * FROM Credentials WHERE name = 'edu'");
		// while (rs.next()) {
		// l.logging(rs.getString(1));
		// l.logging(rs.getString(2));
		// }
		// } catch (SQLException e) {
		// throw new RuntimeException("Cannot connect the database!", e);
		// } finally {
		// System.out.println("Closing the connection.");
		// if (db.getConnection() != null)
		// try {
		// db.getConnection().close();
		// } catch (SQLException ignore) {
		// }
		// }
		VideoData vdata = db.getInfo(222, null, null, l);
		vdata.print(null);

	}

	private void startConnection() throws SQLException {
		this.connection = DriverManager.getConnection(getUrl(), getUsername(),
				getPassword());
	}

	private Connection getConnection() {
		return connection;
	}

	public static String getUrl() {
		return url;
	}

	public static void setUrl(String url) {
		DBManager.url = url;
	}

	public static String getUsername() {
		return username;
	}

	public static void setUsername(String username) {
		DBManager.username = username;
	}

	public static String getPassword() {
		return password;
	}

	public static void setPassword(String password) {
		DBManager.password = password;
	}

}