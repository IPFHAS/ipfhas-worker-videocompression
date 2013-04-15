package fr.tse.lt2c.satin.IpfhasWorkerVideoCompression;

import java.io.File;
import java.io.InputStream;
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
	 * Data received from the Gearman server
	 */
	private JSONObject dataJson;

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

	/**
	 * Compression modalities
	 */
	private JSONArray compressionMod;
	
	/**
	 * Name of video shots (returned if the worker finish his job)
	 */
	private JSONArray namesVideoShots;

	public byte[] work(String function, byte[] data, GearmanFunctionCallback callback)
			throws Exception {

		try {
			logger.info("----- in VideoCompression -----");

			dataJson = convertDataToJson(data);

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

			createShotDurationArray();

			VideoInitialization videoInit = new VideoInitialization();

			fileFolderPath = new File(FOLDER_PATH);
			videoInit.createVideoFolder(fileFolderPath);

			// Create the folders if they don't exist yet
			String folderMovieCompressed = FOLDER_PATH + "/" + videoName;
			videoInit.createVideoFolder(new File(folderMovieCompressed));

			compressionMod = (JSONArray) dataJson.get("compressionModalities");

			logger.debug("compressionModalities: {}", compressionMod);

			for(int i=0; i< compressionMod.size(); i++) {
				shotsCompression((JSONObject) compressionMod.get(i));
			}

			JSONObject sendBack = new JSONObject();
			sendBack.put("namesVideoShots", namesVideoShots);
			
			return sendBack.toJSONString().getBytes();

		}
		catch(Exception e) {
			logger.error("Bug in VideoCompression: {}", e);
			JSONObject error = new JSONObject();
			error.put("error", e.toString());
			return  error.toJSONString().getBytes();
		}
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
			logger.info("----- in createShotDurationArray -----");

			listDuration = new JSONArray();

			for(int i=0; i<listShots.size()-1; i++) {
				double shotDuration = timecodeToSeconds(listShots.get(i+1).toString()) 
						- timecodeToSeconds(listShots.get(i).toString()); 

				logger.debug("Duration for the Shot {}: {}", i+1, shotDuration);

				listDuration.add(roundDouble(shotDuration, 2));
			}

			logger.debug("listDuration: {}", listDuration.toJSONString());
			logger.debug("listShots size: {}", listShots.size());
			logger.debug("listDuration size: {}", listDuration.size());
		}
		catch(Exception e) {
			logger.error("Bub in createShotDurationArray: {}", e);
		}

	}

	/**
	 * Send the timecode into seconds
	 * @param timecode
	 * @return double
	 */
	protected double timecodeToSeconds(String timecode) {
		try{
			logger.info("----- in timeCodeSeconds -----");

			String[] cutTimecode = timecode.split("/");
			String[] splitTimecode = cutTimecode[0].split(":");

			logger.debug("TimeCode: {}", cutTimecode[0].toString());

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
			logger.error("Bug in timeCodeToSeconds: {}", e);
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
			logger.debug("----- in roundDouble -----");
			DecimalFormat df = new DecimalFormat();
			df.setMaximumFractionDigits(nbDigits);
			df.setMinimumFractionDigits(nbDigits);
			df.setDecimalSeparatorAlwaysShown(true);
			String s = df.format(value);
			return s;
		}
		catch(Exception e) {
			logger.error("Bug in roundDouble: {}", e);
			return null;
		}
	}

	/**
	 * Bash command to launch the compression
	 * @param videoPath
	 * @param beginTime
	 * @param xResolution
	 * @param yResolution
	 * @param bitrate
	 * @param destination
	 */
	private void bash(String videoPath, String beginTime, String shotDuration, String xResolution, String yResolution, String bitrate, String destination) {
		try {
			logger.info("----- in bash -----");

			String cmd = "ffmpeg -threads 4 -i " + videoPath +
					" -ss " + beginTime + 
					" -t " + shotDuration +
					" -s " + xResolution + "x" + yResolution +
					" -b " + bitrate + "k " +
					" " + destination;
			// Debug
			logger.debug("bash command: {}", cmd);

			// Call the bash
			this.execBash(cmd);

		}
		catch(Exception e) {
			logger.error("BUG: {}", e);
		}
	}
	
	/**
	 * Bash command to launch the compression
	 * @param videoPath
	 * @param beginTime
	 * @param xResolution
	 * @param yResolution
	 * @param bitrate
	 * @param destination
	 */
	private void bashEnd(String videoPath, String beginTime, String xResolution, String yResolution, String bitrate, String destination) {
		try {
			logger.info("----- in bash -----");

			String cmd = "ffmpeg -threads 4 -i " + videoPath +
					" -ss " + beginTime + 
					" -s " + xResolution + "x" + yResolution +
					" -b " + bitrate + "k " +
					" " + destination;
			// Debug
			logger.debug("bash command: {}", cmd);

			// Call the bash
			this.execBash(cmd);

		}
		catch(Exception e) {
			logger.error("BUG: {}", e);
		}
	}

	/**
	 * Execute the bash command
	 * @param cmd String of the command to to execute
	 */
	@SuppressWarnings("unused")
	private void execBash(String cmd) {
		try {
			logger.info("----- in execBash -----");

			ProcessBuilder pb = new ProcessBuilder("zsh", "-c", cmd);
			pb.redirectErrorStream(true);
			Process shell = pb.start();
			InputStream shellIn = shell.getInputStream();
			int shellExitStatus = shell.waitFor();
			int c;
			while((c = shellIn.read()) != -1) {
				System.out.write(c);
			}

		}
		catch(Exception e) {
			logger.error("Bug in  execBash: {}", e);

		}
	}

	/**
	 * 
	 * 
	 * @param modalities List of the compression modalities
	 */
	private void shotsCompression(JSONObject modalities) {

		logger.debug("----- in shotsCompression -----");
		logger.debug("Modalities to use: {}", modalities.toJSONString());

		try {

			logger.debug("xResolution: {}", modalities.get("xResolution").toString());
			logger.debug("yResolution: {}", modalities.get("yResolution").toString());
			logger.debug("bitrate: {}", modalities.get("bitrate").toString());

			String destination = fileFolderPath.getAbsolutePath() + "/" +
					videoName +
					"/mod_" + 
					modalities.get("xResolution").toString() +
					'_' + modalities.get("yResolution").toString() +
					'_' + modalities.get("bitrate").toString();

			logger.debug("Destination folder of shots: {}", destination);

			File dest = new File(destination);

			if(!dest.exists()) {
				dest.mkdirs();
				logger.debug("Shots folder created");
			}
			
			String videoExtension = dataJson.get("videoExtension").toString();
			
			namesVideoShots = new JSONArray();

			for(int i=0; i<=listShots.size()-1; i++) {

				listShots.set(i, String.valueOf(timecodeToSeconds(listShots.get(i).toString())));
				
				if(i==0) {

					logger.debug("in for loop 1 with i={}", i);
					
					String destination_0 = destination + "/" + videoName + "_" + i + "." + videoExtension;
					
					logger.debug("File destination: {}", destination_0);
					
					bash(videoAddress, "0", 
							listShots.get(0).toString(), 
							modalities.get("xResolution").toString(), 
							modalities.get("yResolution").toString(),
							modalities.get("bitrate").toString(),
							destination_0);
					
					namesVideoShots.add(destination_0);
				}

				if(0<i && i<listShots.size()-1) {
					
					logger.debug("in for loop 2 with i={}", i);
					
					String destination_i = destination + "/" + videoName + "_" + i + "." + videoExtension;
					
					logger.debug("File destination: {}", destination_i);
					
					logger.debug("Begin Time: " + listShots.get(i-1).toString());
					
					bash(videoAddress, listShots.get(i-1).toString(),
							listDuration.get(i-1).toString().replace(",", "."),
							modalities.get("xResolution").toString(), 
							modalities.get("yResolution").toString(),
							modalities.get("bitrate").toString(),
							destination_i);	
					
					namesVideoShots.add(destination_i);
				}

				if(i==listShots.size()-1) {
					
					logger.debug("in for loop 3 with i={}", i);
					
					String destination_end = destination + "/" + videoName + "_" + i + "." + videoExtension;
					
					logger.debug("File destination: {}", destination_end);
					
					bashEnd(videoAddress, listShots.get(i-1).toString(),
							modalities.get("xResolution").toString(), 
							modalities.get("yResolution").toString(),
							modalities.get("bitrate").toString(),
							destination_end);
					
					namesVideoShots.add(destination_end);
				}
			}

		}
		catch(Exception e) {
			logger.error("Bug in shotCompression: {}", e);
		}


	}
}
