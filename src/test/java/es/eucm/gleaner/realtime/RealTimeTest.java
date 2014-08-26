package es.eucm.gleaner.realtime;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import es.eucm.gleaner.realtime.data.PendingRTAnalysis;
import redis.clients.jedis.Jedis;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;

import com.mongodb.DB;

import es.eucm.gleaner.realtime.filters.WriteMongoFilter;
import es.eucm.gleaner.realtime.spouts.TracesSpout;
import es.eucm.gleaner.realtime.topologies.RealtimeTopology;
import es.eucm.gleaner.realtime.utils.DBUtils;

import java.util.Random;

public class RealTimeTest {

	private static StormTopology buildTopology() {
		RealtimeTopology realtimeTopology = new RealtimeTopology(
				new TracesSpout());

		realtimeTopology.getZoneStream().each(
				new Fields("versionId", "gameplayId", "zone"),
				new WriteMongoFilter("rt_results_", "versionId", "gameplayId",
						"zone"));

		return realtimeTopology.build();
	}

	public static void main(String[] args) {
		Config conf = new Config();
		conf.put("mongoHost", "localhost");
		conf.put("mongoPort", 27017);
		conf.put("mongoDB", "test");
		conf.put("jedisHost", "localhost");
		conf.put("jedisSelect", 3);

		prepareDBs(conf);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("realtime", conf, buildTopology());
	}

	private static void prepareDBs(Config conf) {
		Jedis jedis = DBUtils.getJedis(conf);
		jedis.flushDB();

		DB db = DBUtils.getMongoDB(conf);
		db.dropDatabase();

		int gameplays = 7;
		int zones = 15;

		String versionId = "Game";

		Random random = new Random(System.currentTimeMillis());
		DBCollection collection = db.getCollection("traces_" + versionId);
		for (int i = 0; i < 1000; i++) {
			BasicDBObject trace = new BasicDBObject("gameplayId", "gameplay"
					+ random.nextInt(gameplays));
			trace.put("event", "zone");
			trace.put("value", "zone" + random.nextInt(zones));
			collection.insert(trace);
		}

		PendingRTAnalysis pending = new PendingRTAnalysis(jedis, "q_realtime");
		pending.push(versionId);
	}
}
