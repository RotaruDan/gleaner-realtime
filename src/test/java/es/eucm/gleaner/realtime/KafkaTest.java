package es.eucm.gleaner.realtime;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import es.eucm.gleaner.realtime.states.MongoStateFactory;
import es.eucm.gleaner.realtime.topologies.KafkaTopology;
import es.eucm.gleaner.realtime.utils.DBUtils;

import java.util.Map;

public class KafkaTest {

	private static StormTopology buildTopology(Map conf, String version) {
		DBUtils.startRealtime(DBUtils.getMongoDB(conf), version);

		KafkaTopology kafkaTopology = new KafkaTopology(version);
		kafkaTopology.prepare(new MongoStateFactory(), "localhost:2181");
		return kafkaTopology.build();
	}

	public static void main(String[] args) {
		Config conf = new Config();
		conf.put("mongoHost", "localhost");
		conf.put("mongoPort", 27017);
		conf.put("mongoDB", "gleaner");

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("realtime", conf, buildTopology(conf, args[0]));
	}

}

