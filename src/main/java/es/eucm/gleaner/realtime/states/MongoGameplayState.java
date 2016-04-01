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
package es.eucm.gleaner.realtime.states;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBObject;
import es.eucm.gleaner.realtime.utils.DBUtils;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.state.OpaqueValue;

import java.util.List;

public class MongoGameplayState extends GameplayState {
	private static final Logger LOG = LoggerFactory
			.getLogger(GameplayState.class);

	private DB db;

	public MongoGameplayState(DB db) {
		this.db = db;
	}

	@Override
	public void setProperty(String versionId, String gameplayId, String key,
			Object value) {
		ObjectId _id = new ObjectId(gameplayId);
		try {
			DBUtils.getRealtimeResults(db, versionId).update(
					new BasicDBObject("_id", _id),
					new BasicDBObject("$set", new BasicDBObject(key, value)),
					true, false);
		} catch (Exception e) {
			LOG.error("Error setting property " + key + "=" + value, e);
		}
	}

	@Override
	public void setOpaqueValue(String versionId, String gameplayId,
			List<Object> keys, OpaqueValue value) {

		setProperty(versionId, gameplayId, buildKey(keys), value.getCurr());
		String key = toKey(gameplayId, keys);
		try {
			DBUtils.getOpaqueValues(db, versionId).update(
					new BasicDBObject("key", key),
					new BasicDBObject("$set", new BasicDBObject("value",
							toDBObject(value))), true, false);
		} catch (Exception e) {
			LOG.error("Error setting property " + key + "=" + value, e);
		}
	}

	@Override
	public OpaqueValue getOpaqueValue(String versionId, String gameplayId,
			List<Object> keys) {
		String key = toKey(gameplayId, keys);
		DBObject object = DBUtils.getOpaqueValues(db, versionId).findOne(
				new BasicDBObject("key", key));
		return object == null ? null : toOpaqueValue(object);
	}

	private String toKey(String gameplayId, List<Object> key) {
		String result = gameplayId;
		for (Object o : key) {
			result += o;
		}
		return result;
	}

	private DBObject toDBObject(OpaqueValue value) {
		BasicDBObject dbObject = new BasicDBObject();
		dbObject.put("txid", value.getCurrTxid());
		dbObject.put("prev", value.getPrev());
		dbObject.put("curr", value.getCurr());
		return dbObject;
	}

	private String buildKey(List<Object> keys) {
		String result = "";
		for (Object key : keys) {
			result += key + ".";
		}
		return result.substring(0, result.length() - 1);
	}

	private DBObject buildDBObject(List<Object> keys, int i, Object value) {
		if (i == keys.size() - 1) {
			return new BasicDBObject(keys.get(i) + "", value);
		} else {
			return new BasicDBObject(keys.get(i) + "", buildDBObject(keys,
					i + 1, value));
		}
	}

	private OpaqueValue toOpaqueValue(DBObject dbObject) {
		DBObject opaqueValue = (DBObject) dbObject.get("value");
		return new OpaqueValue((Long) opaqueValue.get("txid"),
				opaqueValue.get("curr"), opaqueValue.get("prev"));
	}

}
