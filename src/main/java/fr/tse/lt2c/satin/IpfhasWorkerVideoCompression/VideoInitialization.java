package fr.tse.lt2c.satin.IpfhasWorkerVideoCompression;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URL;

/**
 * Class VideoInitialization
 * Objectives:
 * - Video Folder creation
 * - Download the video to be processed
 * 
 * @author Antoine Lavignotte
 * @version 1.0
 */
public class VideoInitialization extends VideoCompression {

	/**
	 * Logger
	 */
	private static final Logger logger = LoggerFactory.getLogger(VideoInitialization.class);
	
	/**
	 * Constructor
	 */
	public VideoInitialization() {		
	}

	/**
	 * Create a folder to store the video file
	 * @return Boolean True if the folder creation has been well done
	 */
	public boolean createVideoFolder(File folder) {
		try {
			logger.info("---- In createVideoFolder ----");
			if(!folder.exists()) {
				folder.mkdirs();
			}
			return true;
		}
		catch(Exception e) {
			logger.error("Bug in createVideoFolder: {}", e);
			return false;
		}
	}
	
	public boolean copyVideoFromUrl(URL videoUrl, File videoPath) {
		try {
			logger.info("Begin to download the video from url: {}", videoUrl.toString());
			org.apache.commons.io.FileUtils.copyURLToFile(videoUrl, videoPath);
			logger.info("Download finished");
			return true;
		}
		catch(Exception e) {
			logger.error("Bug in copyVideoFromUrl: {}", e);
			return false;
		}
	}
}
