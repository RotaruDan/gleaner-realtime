package es.eucm.gleaner.realtime.states;

import backtype.storm.task.IMetricsContext;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import org.apache.log4j.Logger;
import storm.trident.state.OpaqueValue;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.TransactionalValue;
import storm.trident.state.map.CachedMap;
import storm.trident.state.map.IBackingMap;
import storm.trident.state.map.NonTransactionalMap;
import storm.trident.state.map.OpaqueMap;
import storm.trident.state.map.TransactionalMap;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MongoState<T> implements IBackingMap<T> {

	private DBCollection collection;
	private MongoStateConfig config;
	private static final Logger logger = Logger.getLogger(MongoState.class);

	MongoState(final MongoStateConfig config) {
		this.config = config;
		// initialize the mongo client
		try {
			collection = new MongoClient(config.getHost(), config.getPort())
					.getDB(config.getDb())
					.getCollection(config.getCollection());
		} catch (final UnknownHostException ex) {
			logger.error("DB connection initialization failed.", ex);
		}
	}

	/**
	 * factory method for the factory
	 * 
	 * @param config
	 * @return
	 */
	public static Factory newFactory(final MongoStateConfig config) {
		return new Factory(config);
	}

	/**
	 * multiget implementation for mongodb
	 * 
	 */
	@Override
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public List<T> multiGet(final List<List<Object>> keys) {
		// convert all keys into mongodb id docs
		final List<DBObject> ids = keysToIds(keys);
		List<DBObject> docs = null;
		if (config.isBulkGets()) {
			docs = batchGet(ids);
		} else {
			docs = singleGet(ids);
		}
		// convert the docs into a map so we can figure out which keys are
		// present
		final Map<DBObject, DBObject> docMap = Maps.uniqueIndex(docs,
				new Function<DBObject, DBObject>() {
					@Override
					public DBObject apply(final DBObject doc) {
						return (DBObject) doc.get("_id");
					}
				});
		return Lists.transform(ids, new Function<DBObject, T>() {
			@Override
			public T apply(final DBObject id) {
				final DBObject doc = docMap.get(id);
				if (doc == null) {
					return null;
				} else {
					switch (config.getType()) {
					case OPAQUE:
						return (T) new OpaqueValue(
								(Long) doc.get("txid"),
								documentToValue((DBObject) doc.get("val"),
										Arrays.asList(config.getValueFields())),
								documentToValue((DBObject) doc.get("prev"),
										Arrays.asList(config.getValueFields())));
					case TRANSACTIONAL:
						return (T) new TransactionalValue((Long) doc
								.get("txid"), documentToValue(
								(DBObject) doc.get("val"),
								Arrays.asList(config.getValueFields())));
					default:
						return (T) documentToValue((DBObject) doc.get("val"),
								Arrays.asList(config.getValueFields()));
					}
				}
			}
		});
	}

	/**
	 * multiput implementation for mongodb
	 * 
	 */
	@Override
	public void multiPut(final List<List<Object>> keys, final List<T> values) {
		// convert all keys into mongodb id docs
		final List<DBObject> ids = keysToIds(keys);
		for (int i = 0; i < ids.size(); i++) {
			// perform an upsert
			collection.update(new BasicDBObject("_id", ids.get(i)),
					tuplesToDocument(ids.get(i), values.get(i)), true, false);
		}
		logger.debug(String.format("%1$d keys flushed", keys.size()));
	}

	/**
	 * retrieve all keys in one request
	 * 
	 * @param ids
	 * @return
	 */
	private List<DBObject> batchGet(final List<DBObject> ids) {
		final DBCursor cursor = collection.find(new BasicDBObject("_id",
				new BasicDBObject("$in", ids)));
		try {
			return cursor.toArray();
		} finally {
			if (cursor != null) {
				cursor.close();
			}
		}
	}

	/**
	 * retrieve all keys by issuing single requests iteratively
	 * 
	 * @param ids
	 * @return
	 */
	private List<DBObject> singleGet(final List<DBObject> ids) {
		return Lists.transform(ids, new Function<DBObject, DBObject>() {
			@Override
			public DBObject apply(final DBObject id) {
				return collection.findOne(new BasicDBObject("id", id));
			}
		});
	}

	/**
	 * convert trident keys to mongodb docs repesenting the document id
	 * 
	 * @param keys
	 * @return
	 */
	private List<DBObject> keysToIds(final List<List<Object>> keys) {
		return Lists.transform(keys, new Function<List<Object>, DBObject>() {
			@Override
			public DBObject apply(final List<Object> key) {
				final BasicDBObject id = new BasicDBObject(key.size());
				for (int i = 0; i < config.getKeyFields().length; i++) {
					id.put(config.getKeyFields()[i], key.get(i));
				}
				return id;
			}
		});
	}

	/**
	 * convert an id, value pair to a mongo doc
	 * 
	 * @param id
	 * @param value
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	private DBObject tuplesToDocument(final DBObject id, final T value) {
		final BasicDBObject doc = new BasicDBObject("_id", id);
		switch (config.getType()) {
		case OPAQUE:
			doc.append(
					"val",
					valueToDocument(((OpaqueValue) value).getCurr(),
							Arrays.asList(config.getValueFields())))
					.append("txid", ((OpaqueValue) value).getCurrTxid())
					.append("prev",
							valueToDocument(((OpaqueValue) value).getPrev(),
									Arrays.asList(config.getValueFields())));
			break;
		case TRANSACTIONAL:
			doc.append(
					"val",
					valueToDocument(((TransactionalValue) value).getVal(),
							Arrays.asList(config.getValueFields()))).append(
					"txid", ((TransactionalValue) value).getTxid());
			break;
		default:
			doc.append(
					"val",
					valueToDocument(value,
							Arrays.asList(config.getValueFields())));
		}
		return doc;
	}

	/**
	 * converts a value sub-document to a value tuple(list of objects) ordered
	 * based on value fields if there is a single value field, we return the
	 * object itself rather than a singleton list
	 * 
	 * @param doc
	 * @param valueFields
	 * @return
	 */
	private Object documentToValue(final DBObject doc,
			final List<String> valueFields) {
		// returns either a single value or a tuple based on the value fields
		if (valueFields.size() == 1) {
			return doc.get(valueFields.get(0));
		} else {
			return Lists.transform(valueFields, new Function<String, Object>() {
				@Override
				public Object apply(final String field) {
					return doc.get(field);
				}
			});
		}
	}

	/**
	 * converts a value to sub-document
	 * 
	 * @param value
	 * @param valueFields
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private DBObject valueToDocument(final Object value,
			final List<String> valueFields) {
		// if there's only one value field, we map the value field name directly
		// to the value
		// otherwise we take the value field list as key and the value is
		// assumed to be a list of equal length and constuct a map out of it
		if (valueFields.size() == 1) {
			return new BasicDBObject(valueFields.get(0), value);
		} else {
			final Map<String, Object> valueMap = new HashMap<String, Object>();
			for (int i = 0; i < valueFields.size(); i++) {
				valueMap.put(valueFields.get(i), value == null ? null
						: ((List<Object>) value).get(i));
			}
			return new BasicDBObject(valueMap);
		}
	}

	@SuppressWarnings("serial")
	static class Factory implements StateFactory {
		private MongoStateConfig config;

		Factory(final MongoStateConfig config) {
			this.config = config;
		}

		@Override
		@SuppressWarnings({ "rawtypes", "unchecked" })
		public State makeState(final Map conf, final IMetricsContext context,
				final int partitionIndex, final int numPartitions) {
			final CachedMap map = new CachedMap(new MongoState(config),
					config.getCacheSize());
			switch (config.getType()) {
			case OPAQUE:
				return OpaqueMap.build(map);
			case TRANSACTIONAL:
				return TransactionalMap.build(map);
			default:
				return NonTransactionalMap.build(map);
			}
		}
	}
}
