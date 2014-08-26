package es.eucm.gleaner.realtime.topologies;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Fields;
import com.mongodb.BasicDBObject;
import org.junit.After;
import org.junit.Test;
import storm.trident.testing.FeederBatchSpout;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class RealtimeTopologyTest {

	private String resultsFile = "zones.txt";

	@After
	public void tearDown() {
		new File(resultsFile).delete();
	}

	@Test
	public void test() {
		FeederBatchSpout feederBatchSpout = new FeederBatchSpout(Arrays.asList(
				"versionId", "trace"));

		BasicDBObject trace = new BasicDBObject();
		trace.put("gameplayId", "player1");
		trace.put("event", "zone");
		trace.put("value", "zone1");

		RealtimeTopology realtimeTopology = new RealtimeTopology(
				feederBatchSpout);

		realtimeTopology.getZoneStream().each(new Fields("gameplayId", "zone"),
				new WriteToTextFile(resultsFile, "gameplayId", "zone"));

		Config conf = new Config();
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("realtime", conf, realtimeTopology.build());

		feederBatchSpout.feed(Arrays.asList(Arrays.asList("versionId", trace)));

		String fileResult = "";
		try {
			BufferedReader reader = new BufferedReader(new FileReader(
					resultsFile));
			String line;
			while ((line = reader.readLine()) != null) {
				fileResult += line;
			}
            reader.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		assertEquals(fileResult, "player1,zone1,");
	}

}
