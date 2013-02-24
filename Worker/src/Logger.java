import java.util.Date;

public class Logger {

	/**
	 * Class for logging methods
	 * 
	 * @author Eduardo Hernandez Marquina
	 * @author Hector Veiga
	 * @author Gerardo Travesedo
	 * 
	 */
	public Logger() {
	}

	public String substringBetween(String str, String open, String close) {
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

	public int giveMeSeconds(String line, String del1, String del2) {
		String durVid = "";
		String[] durVidPieces;

		durVid = substringBetween(line, del1, del2);
		durVidPieces = durVid.split(":");
		int secs = (Integer.parseInt(durVidPieces[0]) * 3600)
				+ (Integer.parseInt(durVidPieces[1]) * 60)
				+ Integer.parseInt(durVidPieces[2].substring(0, 2));
		return secs;
	}

	// private static void updateStatus(String percentage) {
	// // Update Database Status
	// try {
	// Connection connec = DriverManager.getConnection(
	// "jdbc:mysql://64.131.110.162/luna", "Europa", "a23d578");
	// Statement s = connec.createStatement();
	// String query1 = "UPDATE requests SET status='" + percentage
	// + "' WHERE id='" + rowId + "'";
	// s.executeUpdate(query1);
	// s.close();
	// connec.close();
	// } catch (Exception e) {
	// logging("Exception catch!!!! Something wrong updating status of a request");
	// logging(e.toString());
	// }
	// }

	// private static void updateParameter(String parameter, long quantity) {
	// logging("Updating parameter '" + parameter + "' by " + quantity);
	//
	// // Update Database Parameter
	// try {
	// Connection connec = DriverManager.getConnection(
	// "jdbc:mysql://64.131.110.162/luna", "Europa", "a23d578");
	// Statement s = connec.createStatement();
	// String query1 = "UPDATE parameters SET value=value+"
	// + String.valueOf(quantity) + " WHERE parameter='"
	// + parameter + "'";
	// s.executeUpdate(query1);
	// s.close();
	// connec.close();
	// } catch (Exception e) {
	// logging("Exception catch!!!! Something wrong updating status of a request");
	// logging(e.toString());
	// }
	// }

	public String logging(String lineToLog) {
		Date time = new Date();
		String line = "[" + time.toString() + "] " + lineToLog;
		System.out.println(line);
		return line;
	}

}