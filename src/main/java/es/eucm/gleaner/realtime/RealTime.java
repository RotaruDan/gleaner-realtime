package es.eucm.gleaner.realtime;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import es.eucm.gleaner.realtime.states.MongoStateFactory;
import es.eucm.gleaner.realtime.topologies.KafkaTopology;
import es.eucm.gleaner.realtime.utils.DBUtils;

import java.util.Map;

public class RealTime {

	private static StormTopology buildTopology(Map conf, String sessionId) {
		DBUtils.startRealtime(DBUtils.getMongoDB(conf), sessionId);

		KafkaTopology kafkaTopology = new KafkaTopology(sessionId);
		kafkaTopology.prepare(new MongoStateFactory());
		return kafkaTopology.build();
	}

	public static void main(String[] args) {

		Config conf = new Config();
		conf.setNumWorkers(1);
		conf.setMaxSpoutPending(500);

		conf.put("mongoHost", "localhost");
		conf.put("mongoPort", 27017);
		conf.put("mongoDB", "gleaner");

		if (args.length == 2 && "debug".equals(args[0])) {
			LocalCluster cluster = new LocalCluster();
			String sessionId = args[1];
			cluster.submitTopology(sessionId, conf,
					buildTopology(conf, sessionId));
		} else {
			try {
				String sessionId = args[0];
				System.out.println("Starting analysis of session " + sessionId);
				StormSubmitter.submitTopology(sessionId, conf,
						buildTopology(conf, sessionId));
			} catch (AlreadyAliveException e) {
				e.printStackTrace();
			} catch (InvalidTopologyException e) {
				e.printStackTrace();
			}
		}
	}
}
