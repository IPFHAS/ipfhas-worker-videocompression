package fr.tse.lt2c.satin.IpfhasWorkerVideoCompression;

import java.io.File;
import java.text.DecimalFormat;

import org.gearman.GearmanFunction;
import org.gearman.GearmanFunctionCallback;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VideoCompression extends IpfhasWorkerVideoCompression implements GearmanFunction {

	private static final Logger logger = LoggerFactory.getLogger(VideoCompression.class);

	/**
	 * Video name
	 */
	private String videoName;

	/**
	 * Video address
	 */
	private String videoAddress;

	/**
	 * List of shots
	 */
	private JSONArray listShots;
	
	/**
	 * Shot duration array
	 */
	private JSONArray listDuration;
	
	/**
	 * Video Folder Path
	 */
	private File fileFolderPath;

	public byte[] work(String function, byte[] data, GearmanFunctionCallback callback)
			throws Exception {

		try {
			logger.info("----- in VideoCompression -----");

			JSONObject dataJson = convertDataToJson(data);

			logger.debug("dataJson received: {}", dataJson.toJSONString());

			videoName = dataJson.get("videoName").toString();
			videoAddress = dataJson.get("videoAddress").toString();
			listShots = (JSONArray) dataJson.get("listShots");

			// Remove fade shots
			for(int i=listShots.size()-1; i>=0; i--) {
				if(listShots.get(i).toString().startsWith("Fade")) {
					listShots.remove(i);
				}
			}
			
			logger.debug("listShots without Fade shots: {}", listShots);

			

		}
		catch(Exception e) {
			logger.error("Bug in VideoCompression: {}", e);
		}


		return null;
	}

	/**
	 * Convert a byte[] data into JSONObject data
	 * @param data Data to convert into JSON
	 * @return Data converted into a JSONObject
	 */
	private JSONObject convertDataToJson(byte[] data) {
		try {
			logger.info("---- In convertDataToJson ----");

			String dataString = new String(data);
			Object obj = JSONValue.parse(dataString);
			JSONObject dataJsonObject = (JSONObject) obj;
			return dataJsonObject;
		}
		catch(Exception e) {
			logger.error("Bug in convertDataToJSON: {}", e);
			return null;
		}
	}

	/**
	 * Create a JSONArray of shot's duration and send it to the database
	 */
	@SuppressWarnings("unchecked")
	private void createShotDurationArray() {
		try {
			logger.info("IN CREATESHOTDURATIONARRAY");

			listDuration = new JSONArray();

			for(int i=0; i<listShots.size()-1; i++) {
				double shotDuration = timecodeToSeconds(listShots.get(i+1).toString()) 
						- timecodeToSeconds(listShots.get(i).toString()); 

				logger.debug("Duration for the Shot {}: {}", i+1, shotDuration);

				listDuration.add(roundDouble(shotDuration, 2));
			}

			// Debug
			logger.debug("listDuration: {}", listDuration.toJSONString());
			logger.debug("listShots size: {}", listShots.size());
			logger.debug("listDuration size: {}", listDuration.size());

			// Video initialization
			VideoInitialization videoInit = new VideoInitialization();
			
			fileFolderPath = new File(folderPath);
			videoInit.createVideoFolder(fileFolderPath);
			
			// Create the folders if they don't exist yet
			String folderMovieCompressed = folderPath + "/" + videoName;
			videoInit.createVideoFolder(new File(folderMovieCompressed));

			
			

		}
		catch(Exception e) {
			logger.error("BUG: {}", e);
		}

	}
	
	/**
	 * Send the timecode into seconds
	 * @param timecode
	 * @return double
	 */
	protected double timecodeToSeconds(String timecode) {
		try{
			logger.info("IN TIMECODETOSECONDS");
			String[] splitTimecode = timecode.split(":");

			if(splitTimecode.length == 4) {
				double seconds = Integer.parseInt(splitTimecode[0]) * 3600 + 
						Integer.parseInt(splitTimecode[1]) * 60 + 
						Integer.parseInt(splitTimecode[2]) + 
						Integer.parseInt(splitTimecode[3]) * 0.01;
				return seconds;
			}
			else {
				logger.error("The timecode received is not well formed: {}", timecode);
				return 0;
			}
		}
		catch(Exception e) {
			logger.error("BUG: {}", e);
			return 0;
		}
	}
	
	/**
	 * Return a String of a rounded double
	 * @param value to convert
	 * @param nbDigits after comma
	 * @return String
	 */
	protected String roundDouble(double value, int nbDigits) {
		try {
			DecimalFormat df = new DecimalFormat();
			df.setMaximumFractionDigits(nbDigits);
			df.setMinimumFractionDigits(nbDigits);
			df.setDecimalSeparatorAlwaysShown(true);
			String s = df.format(value);
			return s;
		}
		catch(Exception e) {
			logger.error("BUG: {}", e);
			return null;
		}
	}


}