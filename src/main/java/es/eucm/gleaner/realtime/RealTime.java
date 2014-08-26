package es.eucm.gleaner.realtime;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import es.eucm.gleaner.realtime.filters.WriteMongoFilter;
import es.eucm.gleaner.realtime.spouts.TracesSpout;
import es.eucm.gleaner.realtime.topologies.RealtimeTopology;

public class RealTime {

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
		conf.setNumWorkers(1);
		conf.setMaxSpoutPending(500);

		conf.put("mongoHost", "localhost");
		conf.put("mongoPort", 27017);
		conf.put("mongoDB", "gleaner");
		conf.put("jedisHost", "localhost");
        conf.put("jedisSelect", 0);

        try {
            StormSubmitter.submitTopology("realtime", conf, buildTopology());
        } catch (AlreadyAliveException e) {
            e.printStackTrace();
        } catch (InvalidTopologyException e) {
            e.printStackTrace();
        }
    }
}
