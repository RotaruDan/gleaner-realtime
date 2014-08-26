package es.eucm.gleaner.realtime.data;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

public class AnalysisState {

	public static final String REALTIME_ID = "rt_id_";

	private Jedis jedis;

	public AnalysisState(Jedis jedis) {
		this.jedis = jedis;
	}

	public String getRealtimeId(String versionId) {
		return jedis.get(REALTIME_ID + versionId);
	}

	public void setRealtimeId(String versionId, String traceId) {
		String key = REALTIME_ID + versionId;
		boolean done = false;
		while (!done) {
			jedis.watch(key);
			String value = jedis.get(key);
			if (value == null || traceId.compareTo(value) > 0) {
				Transaction transaction = jedis.multi();
				transaction.set(key, traceId);
				done = transaction.exec() != null;
			} else {
				jedis.unwatch();
				done = true;
			}
		}
	}
}
