package fr.tse.lt2c.satin.IpfhasWorkerVideoCompression;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;

/**
 * Class for the MongoDb Connection
 * @author Antoine Lavignotte
 * @version 1.0
 */
public class MongoDbConnection {

	/**
	 * Logger
	 */
	private static final Logger logger = LoggerFactory.getLogger(MongoDbConnection.class);
	
	/**
	 * Mongo connection
	 */
	private MongoClient mongoClient;
	
	/**
	 * @return the mongoClient
	 */
	public MongoClient getMongoClient() {
		return mongoClient;
	}

	/**
	 * @param mongoClient the mongoClient to set
	 */
	public void setMongoClient(MongoClient mongoClient) {
		this.mongoClient = mongoClient;
	}

	/**
	 * @return the mongoDb
	 */
	public DB getMongoDb() {
		return mongoDb;
	}

	/**
	 * @param mongoDb the mongoDb to set
	 */
	public void setMongoDb(DB mongoDb) {
		this.mongoDb = mongoDb;
	}

	/**
	 * @return the mongoCollection
	 */
	public DBCollection getMongoCollection() {
		return mongoCollection;
	}

	/**
	 * @param mongoCollection the mongoCollection to set
	 */
	public void setMongoCollection(DBCollection mongoCollection) {
		this.mongoCollection = mongoCollection;
	}

	/**
	 * @return the mongoCursor
	 */
	public DBCursor getMongoCursor() {
		return mongoCursor;
	}

	/**
	 * @param mongoCursor the mongoCursor to set
	 */
	public void setMongoCursor(DBCursor mongoCursor) {
		this.mongoCursor = mongoCursor;
	}

	/**
	 * Mongo database declaration
	 */
	private DB mongoDb;

	/**
	 * Mongo Collection declaration
	 */
	private DBCollection mongoCollection;
	
	/**
	 * Mongo cursor
	 */
	private DBCursor mongoCursor;
	
	/**
	 * Constructor
	 * @param mongoAddress
	 * @param mongoDb
	 * @param mongoCollection
	 */
	public MongoDbConnection(String mongoAddress, String mongoDb, String mongoCollection) {
		try {
			this.mongoClient = new MongoClient(mongoAddress);
			this.mongoDb = this.mongoClient.getDB(mongoDb);
			this.mongoCollection = this.mongoDb.getCollection(mongoCollection);
		}
		catch(Exception e) {
			logger.error("Bug in the MongoDbConnection constructor");
		}	
	}

	/**
	 * Add an information in the database MongoDB
	 * @param where Name of the variable used in 'where' research
	 * @param whereValue Value Value of the variable used in 'where' research
	 * @param what Name of the variable to add
	 * @param whatValue Value of the variable to add
	 * @return boolean True is the insertion has been well done
	 */
	protected boolean addInMongoDb(String where, Object whereValue, String what, Object whatValue) {
		try {
			logger.info("---- in addInMongoDb ----");
			mongoCollection.update(
					new BasicDBObject(where, whereValue), 
					new BasicDBObject(
							"$set", 
							new BasicDBObject(what, whatValue)
							)
					);
			return true;
		}
		catch(Exception e) {
			logger.error("Bug in addInMongoDb: {}", e);
			return false;
		}
	}
	
	/**
	 * Find an information in the database MongoDB
	 * @param where Name of the variable used in 'where' research
	 * @param whereValue Value of the variable used in 'where' research
	 * @param what Variable searched
	 * @return Object which contains the find result
	 */
	protected Object findInMongoDb(String where, Object whereValue, String what) {
		try {
			logger.info("---- in findInMongoDb ----");
			mongoCursor = mongoCollection.find(
					new BasicDBObject(where, whereValue),
					new BasicDBObject(what, 1));
			Object result = null;
			while(mongoCursor.hasNext()) {
				result = mongoCursor.next().get(what);
			}
			return result;
		}
		catch(Exception e) {
			logger.error("Error in findInMongoDb: {}", e);
			return null;
		}
	}
	
	/**
	 * Delete an information in the database
	 * @param where Name of the variable to delete
	 * @param whereValue Value of the variable to delete
	 * @return boolean True is the variable has been deleted
	 */
	protected boolean deleteInMongoDb(String where, Object whereValue) {
		try {
			logger.info("---- in deleteInMongoDb ----");
			mongoCollection.remove(new BasicDBObject(where, whereValue));
			return true;
		}
		catch(Exception e) {
			logger.error("Bug in deleteInMongoDb: {}", e);
			return false;
		}
	}
}
