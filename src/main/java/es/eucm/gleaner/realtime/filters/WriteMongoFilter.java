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
package es.eucm.gleaner.realtime.filters;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import es.eucm.gleaner.realtime.utils.DBUtils;
import storm.trident.operation.Filter;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.Map;

public class WriteMongoFilter implements Filter {

	private DB db;

	private String collectionName;

	private String collectionSuffixField;

	private String index;

	private String[] fields;

	public WriteMongoFilter(String collectionName,
			String collectionSuffixField, String index, String... fields) {
		this.collectionName = collectionName;
		this.collectionSuffixField = collectionSuffixField;
		this.index = index;
		this.fields = fields;
	}

	@Override
	public boolean isKeep(TridentTuple tuple) {

		Object suffix = tuple.getValueByField(collectionSuffixField);

		BasicDBObject query = new BasicDBObject(index,
				tuple.getValueByField(index));

		BasicDBObject update = new BasicDBObject();
		update.put(index, tuple.getValueByField(index));
		for (String field : fields) {
			if (!collectionSuffixField.equals(field)) {
				update.put(field, tuple.getValueByField(field));
			}
		}
		db.getCollection(collectionName + suffix).update(query, update, true,
				false);
		return false;
	}

	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		db = DBUtils.getMongoDB(conf);
	}

	@Override
	public void cleanup() {

	}
}
