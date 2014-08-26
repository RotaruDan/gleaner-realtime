package es.eucm.gleaner.realtime.utils;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import redis.clients.jedis.Jedis;

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

	public static Jedis getJedis(Map conf) {
		Jedis jedis = new Jedis((String) conf.get("jedisHost"));
		jedis.select(((Number) conf.get("jedisSelect")).intValue());
		return jedis;
	}
}
