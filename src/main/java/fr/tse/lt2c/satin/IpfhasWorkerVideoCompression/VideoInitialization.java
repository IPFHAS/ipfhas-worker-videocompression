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
	
	/**
	 * Delete a folder
	 * @param path Where the directory is
	 */
	public void deleteFolder(File path) {
		try {
			logger.info("IN DELETEFOLDER");
			if(path.exists()){
				deleteDir(path);
			}
		}
		catch(Exception e) {
			logger.error("BUG: {}", e);
		}
	}

	/**
	 * Navigate in the directory to find children and to delete them
	 * @param dir Directory you want to delete
	 * @return boolean
	 */
	private static boolean deleteDir(File dir) {
		try {
			logger.info("IN DELETEDIR");
			if (dir.isDirectory()) {
				String[] children = dir.list();
				for (int i=0; i<children.length; i++) {
					boolean success = deleteDir(new File(dir, children[i]));
					if (!success) {
						return false;
					}
				}
			}
		}
		catch(Exception e) {
			logger.error("BUG: {}", e);
		}

		// The directory is now empty so delete it
		return dir.delete();
	}
}
