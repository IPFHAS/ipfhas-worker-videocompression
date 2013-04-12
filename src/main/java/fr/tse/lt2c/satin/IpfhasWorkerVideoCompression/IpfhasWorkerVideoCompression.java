package fr.tse.lt2c.satin.IpfhasWorkerVideoCompression;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Worker for video compression
 * (called by a Gearman call named "videoCompression")
 * 
 * @author Antoine Lavignotte
 * @version 1.0
 */
public class IpfhasWorkerVideoCompression {

	/**
	 * Logger
	 */
	private static final Logger logger = LoggerFactory.getLogger(IpfhasWorkerVideoCompression.class);

	/** 
	 * Worker name 
	 */
	private static final String FUNCTION_NAME = "videoCompression";

	/** 
	 * Gearman Server Address 
	 */
	private static final String SERVER_HOST = "localhost";

	/**
	 *  Port number of the Gearman Server
	 */
	private static final int SERVER_PORT = 4730;

	/** 
	 * Mongo ip address
	 */
	private static final String MONGODB_ADDRESS = "localhost";

	/**
	 * Mongo database
	 */
	private static final String MONGODB_DB = "ipfhas";

	/**
	 * Mongo collection
	 */
	private static final String MONGODB_COLLECTION = "movies";

	/**
	 * MongoDb connection
	 */
	private static MongoDbConnection mongoConn;

	/**
	 * Folder Path
	 */
	protected static final String folderPath = "Videos";

	/**
	 * Get the mongo connection
	 * @return the mongoConn
	 */
	public static MongoDbConnection getMongoConn() {
		return mongoConn;
	}

	/**
	 * Gearman connection
	 */
	private static GearmanConnection gearConnect;

	/**
	 * Get the gearman connection
	 * @return the gearConnect
	 */
	public static GearmanConnection getGearConnect() {
		return gearConnect;
	}

	/**
	 * Main
	 * @param args
	 */
	public static void main(String[] args) {

		try {
			// Info
			logger.info("---- In IpfhasWorkerShotDetection ----");

			// Connection to the Mongo Database
			mongoConn = new MongoDbConnection(
					MONGODB_ADDRESS,
					MONGODB_DB,
					MONGODB_COLLECTION);

			//Worker declaration & connection to the Gearman Server
			gearConnect = new GearmanConnection(
					FUNCTION_NAME, 
					SERVER_HOST, 
					SERVER_PORT);	
		}
		catch(Exception e) {
			logger.error("Bug in main: {}", e);
		}

	}

}
