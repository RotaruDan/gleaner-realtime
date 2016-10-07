/**
 * Copyright (C) 2016 e-UCM (http://www.e-ucm.es/)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package es.eucm.rage.realtime;

import es.eucm.rage.realtime.states.ESGameplayState;
import es.eucm.rage.realtime.states.ESStateFactory;
import es.eucm.rage.realtime.topologies.RealtimeTopology;
import es.eucm.rage.realtime.utils.Document;
import es.eucm.rage.realtime.utils.EsConfig;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.task.IMetricsContext;
import org.apache.storm.trident.state.OpaqueValue;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.testing.FeederBatchSpout;
import org.apache.storm.trident.tuple.TridentTuple;
import org.elasticsearch.client.transport.TransportClient;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import static org.junit.Assert.assertEquals;

public class RealtimeTopologyTest {

	private static final Logger LOG = Logger
			.getLogger(RealtimeTopologyTest.class.getName());

	@Test
	public void test() throws IOException {
		FeederBatchSpout tracesSpout = new FeederBatchSpout(Arrays.asList(
				"versionId", "trace"));

		RealtimeTopology topology = new RealtimeTopology();
		Factory factory = new Factory(new EsConfig("testEsHost",
				"testSessionId"));
		topology.prepareTest(tracesSpout, factory);

		Config conf = new Config();
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("realtime", conf, topology.build());

		BufferedReader reader = new BufferedReader(new InputStreamReader(
				ClassLoader.getSystemResourceAsStream("traces.txt")));

		String line;
		ArrayList<List<Object>> tuples = new ArrayList<List<Object>>();
		while ((line = reader.readLine()) != null) {
			tuples.add(Arrays.asList("version", buildTrace(line)));
			LOG.info(line);

		}
		tracesSpout.feed(tuples);
		for (Document document : Factory.state.documents) {
			LOG.info(document.toString());
		}

		assertEquals(10, Factory.state.documents.size());

		Player player = Factory.state.getPlayer("1");
		assertEquals("10", player.properties.get("NotaAT"));
		assertEquals("16", player.properties.get("NotaDT"));
		assertEquals("13", player.properties.get("NotaINC"));

		Player player2 = Factory.state.getPlayer("2");
		assertEquals("54", player2.properties.get("NotaAT"));
		assertEquals("65", player2.properties.get("NotaDT"));
		assertEquals("19", player2.properties.get("NotaINC"));

	}

	private Map<String, Object> buildTrace(String line) {
		String[] parts = line.split(",");
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("gameplayId", parts[0]);
		map.put("timestamp", parts[1]);
		map.put("event", parts[2]);
		map.put("type", parts[3]);
		map.put("target", parts[4]);
		if (5 < parts.length) {
			map.put("response", parts[5]);
		}
		if (7 < parts.length) {
			map.put(parts[6], parts[7]);
		}
		return map;
	}

	public static class TestState extends ESGameplayState {

		private Map<String, Player> players = new HashMap<String, Player>();

		public List<Document> documents = new ArrayList<>();

		public TestState(TransportClient client, String sessionId) {
			super(client, sessionId);
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

		@Override
		public void bulkUpdateIndices(List<TridentTuple> inputs) {

			for (TridentTuple input : inputs) {
				Document<Map> doc = (Document<Map>) input
						.getValueByField("document");
				documents.add(doc);

				String key = "NotaDT";
				Object score = doc.getSource().get(key);
				if (score != null) {
					getPlayer(doc.getSource().get("gameplayId").toString())
							.setProperty(key, doc.getSource().get(key));
				}

				key = "NotaAT";
				score = doc.getSource().get(key);
				if (score != null) {
					getPlayer(doc.getSource().get("gameplayId").toString())
							.setProperty(key, doc.getSource().get(key));
				}

				key = "NotaINC";
				score = doc.getSource().get(key);
				if (score != null) {
					getPlayer(doc.getSource().get("gameplayId").toString())
							.setProperty(key, doc.getSource().get(key));
				}
			}
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
			LOG.info("setProperty " + key + ", " + value);
			properties.put(key, value);
		}

		public void setValue(String key, OpaqueValue value) {
			values.put(key, value);
		}

		public OpaqueValue getValue(String key) {
			return values.get(key);
		}
	}

	public static class Factory extends ESStateFactory {

		public static TestState state;

		public Factory(EsConfig config) {
			super(config);
		}

		@Override
		public State makeState(Map conf, IMetricsContext metrics,
				int partitionIndex, int numPartitions) {
			if (state == null) {
				state = new TestState(null, getConfig().getSessionId());
			}
			return state;
		}
	}

}
