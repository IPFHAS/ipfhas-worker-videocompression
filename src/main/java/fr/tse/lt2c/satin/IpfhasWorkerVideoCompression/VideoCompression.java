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

/**
 * Class for video compression
 * 
 * @author Antoine Lavignotte
 * @version 1.0
 */
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
	private JSONArray listDurations;

	/**
	 * Video Folder Path
	 */
	private File fileFolderPath;

	/**
	 * Compression modalities
	 */
	private JSONArray compressionMod;

	/**
	 * Bitrate / List shots
	 */
	private JSONObject modalitiesListShots;

	/**
	 * Name of video shots (returned if the worker finish his job)
	 */
	private JSONArray listLocationVideoShots;

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
			File folderMovieCompressed = new File(FOLDER_PATH + "/" + videoName);
			if(folderMovieCompressed.exists()) {
				videoInit.deleteFolder(folderMovieCompressed);
			}
			videoInit.createVideoFolder(folderMovieCompressed);

			compressionMod = (JSONArray) dataJson.get("compressionModalities");

			logger.debug("compressionModalities: {}", compressionMod);

			modalitiesListShots = new JSONObject();

			JSONArray list = new JSONArray();
			listLocationVideoShots = new JSONArray();

			for(int i=0; i< compressionMod.size(); i++) {
				JSONObject mod = new JSONObject();
				mod.put("modality", compressionMod.get(i));
				shotsCompression(mod);
				logger.debug("mod: {}", mod);
				logger.debug("listLocationVideoShots: {}", listLocationVideoShots);
				mod.put("listShots", listLocationVideoShots);
				logger.debug("mod: {}", mod);
				String copy = new String(mod.toJSONString());
				list.add(copy);
				listLocationVideoShots.clear();
			}

			logger.debug("list: {}", list);
			
			modalitiesListShots.put("modalitiesListShots", list);

			logger.debug("modalitiesListShots: {}", modalitiesListShots);

			JSONObject sendBack = modalitiesListShots;
			sendBack.put("listDurations", listDurations);

			logger.debug("sendBack: {}", sendBack);

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

			listDurations = new JSONArray();

			for(int i=0; i<listShots.size()-1; i++) {
				double shotDuration = timecodeToSeconds(listShots.get(i+1).toString()) 
						- timecodeToSeconds(listShots.get(i).toString()); 

				logger.debug("Duration for the Shot {}: {}", i+1, shotDuration);

				listDurations.add(roundDouble(shotDuration, 2));
			}

			logger.debug("listDuration: {}", listDurations.toJSONString());
			logger.debug("listShots size: {}", listShots.size());
			logger.debug("listDuration size: {}", listDurations.size());
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
			logger.error("Bug in bash: {}", e);
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
			logger.error("Bug in bashEnd: {}", e);
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
	private int shotsCompression(JSONObject modalities) {

		logger.debug("----- in shotsCompression -----");
		logger.debug("Modalities to use: {}", modalities.toJSONString());

		try {
			JSONObject modality = (JSONObject) modalities.get("modality");
			logger.debug("xResolution: {}", modality.get("xResolution").toString());
			logger.debug("yResolution: {}", modality.get("yResolution").toString());
			logger.debug("bitrate: {}", modality.get("bitrate").toString());

			String destination = fileFolderPath.getAbsolutePath() + "/" +
					videoName +
					"/mod_" + 
					modality.get("xResolution").toString() +
					'_' + modality.get("yResolution").toString() +
					'_' + modality.get("bitrate").toString();

			logger.debug("Destination folder of shots: {}", destination);

			File dest = new File(destination);

			if(!dest.exists()) {
				dest.mkdirs();
				logger.debug("Shots folder created");
			}

			String videoExtension = dataJson.get("videoExtension").toString();

			for(int i=0; i<=listShots.size()-1; i++) {

				listShots.set(i, String.valueOf(timecodeToSeconds(listShots.get(i).toString())));

				if(i==0) {

					logger.debug("in for loop 1 with i={}", i);

					String destination_0 = destination + "/" + videoName + "_" + i + "." + videoExtension;

					logger.debug("File destination: {}", destination_0);

					bash(videoAddress, "0", 
							listShots.get(0).toString(), 
							modality.get("xResolution").toString(), 
							modality.get("yResolution").toString(),
							modality.get("bitrate").toString(),
							destination_0);

					logger.debug("listLocationVideoShots: {}", listLocationVideoShots.toJSONString());
					listLocationVideoShots.add(destination_0);
					logger.debug("listLocationVideoShots: {}", listLocationVideoShots.toJSONString());
				}

				if(0<i && i<listShots.size()-1) {

					logger.debug("in for loop 2 with i={}", i);

					String destination_i = destination + "/" + videoName + "_" + i + "." + videoExtension;

					logger.debug("File destination: {}", destination_i);

					logger.debug("Begin Time: " + listShots.get(i-1).toString());

					bash(videoAddress, listShots.get(i-1).toString(),
							listDurations.get(i-1).toString().replace(",", "."),
							modality.get("xResolution").toString(), 
							modality.get("yResolution").toString(),
							modality.get("bitrate").toString(),
							destination_i);	

					listLocationVideoShots.add(destination_i);
					logger.debug("listLocationVideoShots: {}", listLocationVideoShots.toJSONString());

				}

				if(i==listShots.size()-1) {

					logger.debug("in for loop 3 with i={}", i);

					String destination_end = destination + "/" + videoName + "_" + i + "." + videoExtension;

					logger.debug("File destination: {}", destination_end);

					bashEnd(videoAddress, listShots.get(i-1).toString(),
							modality.get("xResolution").toString(), 
							modality.get("yResolution").toString(),
							modality.get("bitrate").toString(),
							destination_end);

					listLocationVideoShots.add(destination_end);
					logger.debug("listLocationVideoShots: {}", listLocationVideoShots.toJSONString());

				}
			}

			return 1;
		}
		catch(Exception e) {
			logger.error("Bug in shotCompression: {}", e);
			return 0;
		}


	}
}
