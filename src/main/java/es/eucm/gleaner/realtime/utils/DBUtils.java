/**
 * Copyright (C) 2016 e-UCM (http://www.e-ucm.es/)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
