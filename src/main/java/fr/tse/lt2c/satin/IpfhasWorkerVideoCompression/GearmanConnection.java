package fr.tse.lt2c.satin.IpfhasWorkerVideoCompression;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.gearman.Gearman;
import org.gearman.GearmanServer;
import org.gearman.GearmanWorker;

/**
 * Class for the Gearman Connection
 * @author Antoine Lavignotte
 * @version 1.0
 */
public class GearmanConnection {

	/**
	 * Logger
	 */
	private static final Logger logger = LoggerFactory.getLogger(GearmanConnection.class);

	/**  
	 * Gearman declaration 
	 */
	private Gearman gearman;
	
	/**
	 * Gearman server declaration
	 */
	private GearmanServer gearmanServer;
	
	/**
	 * Gearman worker declaration
	 */
	private GearmanWorker gearmanWorker;
	
	/**
	 * Get gearman
	 * @return the gearman
	 */
	public Gearman getGearman() {
		return gearman;
	}

	/**
	 * Get gearmanServer
	 * @return the gearmanServer
	 */
	public GearmanServer getGearmanServer() {
		return gearmanServer;
	}

	/**
	 * Get gearmanWorker
	 * @return the gearmanWorker
	 */
	public GearmanWorker getGearmanWorker() {
		return gearmanWorker;
	}

	/**
	 * Constructor
	 * @param functionName Name of the gearman function to call to execute this worker
	 * @param serverHost Ip address of the Gearman server
	 * @param serverPort Port of the Gearman server
	 */
	public GearmanConnection(String functionName, String serverHost, int serverPort) {

		try {
			logger.info("---- In GearmanConnection ----");

			/*
			 * Create a Gearman instance
			 */
			gearman = Gearman.createGearman();

			/*
			 * Create the job server object. This call creates an object represents
			 * a remote job server.
			 * 
			 * Parameter 1: the host address of the job server.
			 * Parameter 2: the port number the job server is listening on.
			 * 
			 * A job server receives jobs from clients and distributes them to
			 * registered workers.
			 */
			gearmanServer = gearman.createGearmanServer(
					serverHost, serverPort);

			/*
			 * Create a gearman worker. The worker poll jobs from the server and
			 * executes the corresponding GearmanFunction
			 */
			gearmanWorker = gearman.createGearmanWorker();

			/*
			 *  Tell the worker how to perform the ShotDetection function
			 */
			gearmanWorker.addFunction(functionName, new VideoCompression());

			/*
			 *  Tell the worker that it may communicate with the job server
			 */
			gearmanWorker.addServer(gearmanServer);
		}
		catch(Exception e) {
			logger.error("Bug during Gearman Connection: {}", e);
		}
	}
}