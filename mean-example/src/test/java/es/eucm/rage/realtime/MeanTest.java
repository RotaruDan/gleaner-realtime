/**
 * Copyright © 2016 e-UCM (http://www.e-ucm.es/)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package es.eucm.rage.realtime;

import com.google.gson.Gson;
import es.eucm.rage.realtime.example.AverageUpdater;
import es.eucm.rage.realtime.example.topologies.MeanTopologyBuilder;
import es.eucm.rage.realtime.topologies.TopologyBuilder;
import es.eucm.rage.realtime.utils.CSVToMapTrace;
import es.eucm.rage.realtime.utils.ESUtils;
import es.eucm.rage.realtime.states.elasticsearch.EsState;
import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.apache.http.util.EntityUtils;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.testing.FeederBatchSpout;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.util.*;
import java.util.logging.Logger;

import static org.junit.Assert.assertEquals;

/**
 * Tests the {@link MeanTopologyBuilder} executed on a local cluster and
 * receiving data from a local file ("traces.txt")
 */
public class MeanTest {

	private static final String[] VERBS_FILES = { "completable/completed.1",
			"completable/completed.2", "completable/completed.3" };

	private static final String NOW_DATE = "mean-"
			+ String.valueOf(new Date().getTime());
	private static final String ES_HOST = "localhost";
	private static final String ZOOKEEPER_URL = "localhost";

	@Test
	public void test() throws IOException {
		FeederBatchSpout tracesSpout = new FeederBatchSpout(
				Arrays.asList(es.eucm.rage.realtime.topologies.TopologyBuilder.TRACE_KEY));

		TridentTopology topology = new TridentTopology();

		EsState.Factory partitionPersist = new EsState.Factory();

		// Test topology Builder configuration
		new MeanTopologyBuilder().build(topology, null,
				topology.newStream("testFileStream", tracesSpout),
				partitionPersist, null, null);

		Config conf = new Config();
		conf.put(AbstractAnalysis.ZOOKEEPER_URL_FLUX_PARAM, ZOOKEEPER_URL);
		conf.put(AbstractAnalysis.ELASTICSEARCH_URL_FLUX_PARAM, ES_HOST);
		conf.put(AbstractAnalysis.TOPIC_NAME_FLUX_PARAM, NOW_DATE);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("realtime", conf, topology.build());

		CSVToMapTrace parser = new CSVToMapTrace();
		int totalTraces = 0;
		double totalSum = 0;
		String type = "level";
		String event = "completed";
		String firstIndex = NOW_DATE;
		Map<String, Integer> res = new HashMap<>();
		for (int i = 0; i < VERBS_FILES.length; ++i) {
			List<List<Object>> tuples = parser.getTuples("verbs/"
					+ VERBS_FILES[i] + ".csv", firstIndex, i);
			totalTraces += tuples.size();
			tracesSpout.feed(tuples);

			for (List<Object> tuple : tuples) {
				Map trace = (Map) tuple.get(0);
				Map out = (Map) trace.get(TopologyBuilder.OUT_KEY);
				double score = Double.valueOf((String) out
						.get(TopologyBuilder.TridentTraceKeys.SCORE));
				type = out.get(TopologyBuilder.TridentTraceKeys.TYPE)
						.toString();
				event = out.get(TopologyBuilder.TridentTraceKeys.EVENT)
						.toString();
				totalSum += score;
			}
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
				"/" + ESUtils.getTracesIndex(firstIndex) + "/" + type + "/"
						+ event);
		int status = response.getStatusLine().getStatusCode();

		assertEquals("TEST GET error, status is" + status, status,
				HttpStatus.SC_OK);

		String responseString = EntityUtils.toString(response.getEntity());
		Map<String, Object> meanState = (Map) gson.fromJson(responseString,
				Map.class).get("_source");

		int count = ((Double) meanState.get(AverageUpdater.COUNT_KEY))
				.intValue();
		assertEquals(
				"Count completables " + totalTraces + ", current " + count,
				totalTraces, count);

		assertEquals(
				"Total scores sum " + totalSum + ", current "
						+ meanState.get(AverageUpdater.SUM_KEY), totalSum,
				meanState.get(AverageUpdater.SUM_KEY));

	}
}
