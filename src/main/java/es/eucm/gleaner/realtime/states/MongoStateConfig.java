package es.eucm.gleaner.realtime.states;

import storm.trident.state.StateType;

import java.io.Serializable;

@SuppressWarnings("serial")
public class MongoStateConfig implements Serializable {

	private String host;

	private int port;
	private String db;
	private String collection;
	private StateType type = StateType.OPAQUE;
	private String[] keyFields;
	private String[] valueFields;
	private boolean bulkGets = true;
	private int cacheSize = DEFAULT;

	private static final int DEFAULT = 5000;

	public MongoStateConfig(String host, int port, String db, String collection) {
		this.host = host;
		this.port = port;
		this.db = db;
		this.collection = collection;
	}

	public MongoStateConfig(String host, int port, String db,
			String collection, final StateType type, final String[] keyFields,
			final String[] valueFields) {
		this.host = host;
		this.db = db;
		this.collection = collection;
		this.type = type;
		this.keyFields = keyFields;
		this.valueFields = valueFields;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public String getDb() {
		return db;
	}

	public void setDb(String db) {
		this.db = db;
	}

	public String getCollection() {
		return collection;
	}

	public void setCollection(String collection) {
		this.collection = collection;
	}

	public StateType getType() {
		return type;
	}

	public void setType(StateType type) {
		this.type = type;
	}

	public String[] getKeyFields() {
		return keyFields;
	}

	public void setKeyFields(String[] keyFields) {
		this.keyFields = keyFields;
	}

	public boolean isBulkGets() {
		return bulkGets;
	}

	public void setBulkGets(boolean bulkGets) {
		this.bulkGets = bulkGets;
	}

	public int getCacheSize() {
		return cacheSize;
	}

	public void setCacheSize(int cacheSize) {
		this.cacheSize = cacheSize;
	}

	public String[] getValueFields() {
		return valueFields;
	}

	public void setValueFields(String[] valueFields) {
		this.valueFields = valueFields;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}
}
