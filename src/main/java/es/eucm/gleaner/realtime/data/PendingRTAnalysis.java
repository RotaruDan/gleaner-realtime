package es.eucm.gleaner.realtime.data;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

import java.util.Set;

public class PendingRTAnalysis {

	private Jedis jedis;

	private String queueKey;

	public PendingRTAnalysis(Jedis jedis, String queueKey) {
		this.jedis = jedis;
		this.queueKey = queueKey;
	}

	public String pop() {
		Transaction transaction = jedis.multi();
		Response<Set<String>> response = transaction.zrange(queueKey, 0, 0);
		transaction.zremrangeByRank(queueKey, 0, 0);
		transaction.exec();

		for (String result : response.get()) {
			return result;
		}
		return null;
	}

	public void push(String key) {
		jedis.zadd(queueKey, 1.0, key);
	}
}
