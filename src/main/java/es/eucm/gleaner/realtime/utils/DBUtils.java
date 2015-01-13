package es.eucm.gleaner.realtime.utils;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

import java.net.UnknownHostException;
import java.util.Map;

public class DBUtils {

	public static DB getMongoDB(Map conf) {
		try {
			return new MongoClient((String) conf.get("mongoHost"),
					((Number) conf.get("mongoPort")).intValue())
					.getDB((String) conf.get("mongoDB"));
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static DBCollection getMongoCollection(Map conf,
			String collectionName) {
		return getMongoDB(conf).getCollection(collectionName);
	}

	public static DBCollection getRealtimeResults(DB db, String version) {
		return db.getCollection("session" + version);
	}

	public static DBCollection getOpaqueValues(DB db, String version) {
		return db.getCollection("session_opaque_values_" + version);
	}

	public static void startRealtime(DB db, String version) {
		getRealtimeResults(db, version).drop();
		getOpaqueValues(db, version).drop();
		getOpaqueValues(db, version).createIndex(new BasicDBObject("key", 1));
	}
}
