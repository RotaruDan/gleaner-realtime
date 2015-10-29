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

    /**
     * Configures the default values for the 'conf' parameter.
     * @param conf
     * @param mongodbUrl
     */
    private static void setUpConfig(Config conf, String mongodbUrl){
        conf.setNumWorkers(1);
        conf.setMaxSpoutPending(500);

        String partsStr = mongodbUrl.split("://")[1];
        String[] parts = partsStr.split("/");
        String[] hostPort = parts[0].split(":");

        conf.put("mongoHost", hostPort[0]);
        conf.put("mongoPort", Integer.valueOf(hostPort[1]));
        conf.put("mongoDB", parts[1]);
    }

    /**
     *
     * @param args either [<sessionId>, <mongodbUrl>, 'debug'] (local cluster)
     *             or [<sessionId>, <mongodbUrl>] for production mode.
     *             'mongodbUrl' has the following format: 'mongodb://<mongoHost>:<mongoPort>/<mongoDB>'
     */
	public static void main(String[] args) {

		Config conf = new Config();
        String sessionId = args[0];
        setUpConfig(conf, args[1]);

		if (args.length == 3 && "debug".equals(args[2])) {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(sessionId, conf,
					buildTopology(conf, sessionId));
		} else {
			try {
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
