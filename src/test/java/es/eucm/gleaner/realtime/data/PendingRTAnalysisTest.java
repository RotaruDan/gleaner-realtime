package es.eucm.gleaner.realtime.data;

import org.junit.BeforeClass;

import org.junit.Test;
import redis.clients.jedis.Jedis;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class PendingRTAnalysisTest {

	private static Jedis jedis;

	@BeforeClass
	public static void setUpClass() {
		jedis = new Jedis("localhost");
		// Select test "database"
		jedis.select(3);
	}

	@Test
	public void testQueue() {
		PendingRTAnalysis queue = new PendingRTAnalysis(jedis, "q_test");
		queue.push("a");
		assertEquals(queue.pop(), "a");
		assertNull(queue.pop());
	}
}
