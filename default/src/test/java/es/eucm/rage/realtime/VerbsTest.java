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

import com.google.gson.Gson;
import es.eucm.rage.realtime.simple.topologies.TopologyBuilder;
import es.eucm.rage.realtime.utils.CSVToMapTrace;
import es.eucm.rage.realtime.utils.ESUtils;
import es.eucm.rage.realtime.states.elasticsearch.EsMapState;
import es.eucm.rage.realtime.states.elasticsearch.EsState;
import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.apache.http.util.EntityUtils;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.testing.FeederBatchSpout;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Tests the {@link TopologyBuilder} executed on a local cluster and receiving
 * data from a local files VERBS_FILES
 */
public class VerbsTest {

	private static final String[] VERBS_FILES = { "completable/initialized",
			"completable/progressed", "completable/completed",
			"accessible/accessed", "accessible/skipped",
			"alternative/selected", "alternative/unlocked" };

	private static final String NOW_DATE = String.valueOf(new Date().getTime());
	private static final String ES_HOST = "localhost";
	private static final String ZOOKEEPER_URL = "localhost";

	@Test
	public void xAPIVerbsTest() throws IOException {

		FeederBatchSpout tracesSpout = new FeederBatchSpout(Arrays.asList(
				TopologyBuilder.SESSION_ID_KEY,
				es.eucm.rage.realtime.topologies.TopologyBuilder.TRACE_KEY));

		TridentTopology topology = new TridentTopology();

		EsState.Factory partitionPersist = new EsState.Factory();
		StateFactory persistentAggregateFactory = new EsMapState.Factory();

		// Test topology Builder configuration
		new TopologyBuilder().build(topology,
				topology.newStream("testFileStream", tracesSpout),
				partitionPersist, persistentAggregateFactory);

		Config conf = new Config();
		conf.put(AbstractAnalysis.SESSION_ID_FLUX_PARAM, NOW_DATE);
		conf.put(AbstractAnalysis.ZOOKEEPER_URL_FLUX_PARAM, ZOOKEEPER_URL);
		conf.put(AbstractAnalysis.ELASTICSEARCH_URL_FLUX_PARAM, ES_HOST);
		conf.put(AbstractAnalysis.SESSION_ID_FLUX_PARAM, NOW_DATE);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("realtime", conf, topology.build());

		CSVToMapTrace parser = new CSVToMapTrace(NOW_DATE);
		int totalTraces = 0;
		for (int i = 0; i < VERBS_FILES.length; ++i) {
			List tuples = parser.getTuples("verbs/" + VERBS_FILES[i] + ".csv",
					i);
			tracesSpout.feed(tuples);
			totalTraces += tuples.size();
		}

		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		Gson gson = new Gson();

		RestClient client = RestClient.builder(new HttpHost(ES_HOST, 9200))
				.build();

		Response response = client.performRequest("GET",
				"/" + ESUtils.getTracesIndex(NOW_DATE)
						+ "/_search?size=5000&q=*:*");
		int status = response.getStatusLine().getStatusCode();

		assertEquals("TEST GET error, status is" + status, status,
				HttpStatus.SC_OK);

		String responseString = EntityUtils.toString(response.getEntity());
		Map<String, Object> responseDocs = gson.fromJson(responseString,
				Map.class);

		Map hits = (Map) responseDocs.get("hits");

		int total = ((Double) hits.get("total")).intValue();

		assertEquals("Total traces " + totalTraces + ", current " + total,
				totalTraces, total);

		String resultsIndex = ESUtils.getResultsIndex(NOW_DATE);
		for (int i = 0; i < VERBS_FILES.length; ++i) {
			List<String> lines = parser.getLines("verbs/results/"
					+ VERBS_FILES[i] + ".result");

			System.out.println("ROUTE PLAYER: " + resultsIndex + "/"
					+ ESUtils.getResultsType() + "/gameplayid" + i);
			Response resultResponse = client.performRequest("GET", "/"
					+ resultsIndex + "/" + ESUtils.getResultsType()
					+ "/gameplayid" + i);
			int resultStatus = resultResponse.getStatusLine().getStatusCode();
			System.out.println("resultStatus = " + resultStatus);
			assertEquals("TEST GET result error, status is" + resultStatus,
					resultStatus, HttpStatus.SC_OK);

			String responseResultString = EntityUtils.toString(resultResponse
					.getEntity());
			System.out
					.println("responseResultString = " + responseResultString);
			Map<String, Object> playerState = (Map) gson.fromJson(
					responseResultString, Map.class).get("_source");
			System.out.println("playerState = " + playerState);
			for (String line : lines) {
				String[] keyValue = line.split("=");
				String flatObjectKey = keyValue[0];
				String[] keys = flatObjectKey.split("\\.");

				Map propertyMap = playerState;
				for (int j = 0; j < keys.length - 1; ++j) {
					propertyMap = (Map) propertyMap.get(keys[j]);
					assertNotNull("Property Map should not be null for key "
							+ keys[j], propertyMap);
				}
				Object value = propertyMap.get(keys[keys.length - 1]);

				if (flatObjectKey
						.startsWith(es.eucm.rage.realtime.topologies.TopologyBuilder.TraceEventTypes.PROGRESSED)
						|| flatObjectKey
								.startsWith(es.eucm.rage.realtime.topologies.TopologyBuilder.TraceEventTypes.COMPLETED)) {

					try {
						assertEquals(flatObjectKey,
								Double.valueOf(value.toString()),
								Double.valueOf(keyValue[1]));
					} catch (Exception ex) {
						assertEquals(flatObjectKey, value, keyValue[1]);
					}
				} else {
					assertEquals(flatObjectKey, value,
							Double.valueOf(keyValue[1]));
				}
			}
		}

	}
}
