package es.eucm.gleaner.realtime.topologies;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.task.IMetricsContext;
import es.eucm.gleaner.realtime.states.GameplayState;
import org.bson.BSONObject;
import org.junit.Test;
import storm.trident.state.OpaqueValue;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.testing.FeederBatchSpout;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class RealtimeTopologyTest {

	@Test
	public void test() throws IOException {
		FeederBatchSpout tracesSpout = new FeederBatchSpout(Arrays.asList(
				"versionId", "trace"));

		RealtimeTopology topology = new RealtimeTopology();
		Factory factory = new Factory();
		topology.prepare(tracesSpout, factory);

		Config conf = new Config();
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("realtime", conf, topology.build());

		BufferedReader reader = new BufferedReader(new InputStreamReader(
				ClassLoader.getSystemResourceAsStream("traces.txt")));

		String line;
		int i = 0;
		ArrayList<List<Object>> tuples = new ArrayList<List<Object>>();
		while ((line = reader.readLine()) != null) {
			tuples.add(Arrays.asList("version", buildTrace(line)));
			if (i % 5 == 0) {
				tracesSpout.feed(tuples);
				tuples = new ArrayList<List<Object>>();
			}
		}
		tracesSpout.feed(tuples);

		for (int k = 1; k <= 3; k++) {
			Player player = Factory.state.getPlayer(k + "");
			assertEquals(player.properties.get("zone"), "zone" + k);
			for (int j = 1; j < 7; j++) {
				assertEquals("Player" + k + " var" + j + " should be " + k,
						player.properties.get("var.var" + j), k + "");
			}
			assertEquals("Player" + k + " interact should be " + k, player
					.getValue("interacttarget").getCurr(), Long.parseLong(k
					+ ""));
			assertEquals("Player" + k + " choice should be " + k, player
					.getValue("choiceo" + k + "a").getCurr(), Long.parseLong(k
					+ ""));
		}

	}

	private Map<String, Object> buildTrace(String line) {
		String[] parts = line.split(",");
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("gameplayId", parts[0]);
		map.put("event", parts[1]);
		map.put("target", parts[2]);
		map.put("value", parts[3]);
		return map;
	}

	public static class TestState extends GameplayState {

		private Map<String, Player> players = new HashMap<String, Player>();

		protected TestState() {
		}

		@Override
		public void setProperty(String versionId, String gameplayId,
				String key, Object value) {
			getPlayer(gameplayId).setProperty(key, value);
		}

		@Override
		public void setOpaqueValue(String versionId, String gameplayId,
				List<Object> key, OpaqueValue value) {
			getPlayer(gameplayId).setValue(keyFromList(key), value);
		}

		@Override
		public OpaqueValue getOpaqueValue(String versionId, String gameplayId,
				List<Object> key) {
			return getPlayer(gameplayId).getValue(keyFromList(key));
		}

		private String keyFromList(List<Object> keys) {
			String key = "";
			for (Object o : keys) {
				key += o;
			}
			return key;
		}

		private Player getPlayer(String gameplayId) {
			Player player = players.get(gameplayId);
			if (player == null) {
				player = new Player();
				players.put(gameplayId, player);
			}
			return player;
		}
	}

	public static class Player {

		public Map<String, Object> properties = new HashMap<String, Object>();

		public Map<String, OpaqueValue> values = new HashMap<String, OpaqueValue>();

		public void setProperty(String key, Object value) {
			Object o = properties.get(key);
			if (o instanceof BSONObject && value instanceof BSONObject) {
				((BSONObject) o).putAll((Map) value);
			} else {
				properties.put(key, value);
			}
		}

		public void setValue(String key, OpaqueValue value) {
			values.put(key, value);
		}

		public OpaqueValue getValue(String key) {
			return values.get(key);
		}
	}

	public static class Factory implements StateFactory {

		public static TestState state;

		@Override
		public State makeState(Map conf, IMetricsContext metrics,
				int partitionIndex, int numPartitions) {
			if (state == null) {
				state = new TestState();
			}
			return state;
		}
	}

}
