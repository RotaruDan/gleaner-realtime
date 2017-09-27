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

import es.eucm.rage.realtime.example.AverageUpdater;
import es.eucm.rage.realtime.example.topologies.MeanTopologyBuilder;
import es.eucm.rage.realtime.topologies.TopologyBuilder;
import es.eucm.rage.realtime.utils.CSVToMapTrace;
import es.eucm.rage.realtime.utils.ESUtils;
import es.eucm.rage.realtime.states.elasticsearch.EsState;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.testing.FeederBatchSpout;
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

	private static final String NOW_DATE = new Date().toString().toLowerCase()
			.trim().replace(" ", "-");
	private static final String ES_HOST = "localhost";
	private static final String ZOOKEEPER_URL = "localhost";
	private static final Logger LOG = Logger
			.getLogger(MeanTest.class.getName());

	@Test
	public void test() throws IOException {
		FeederBatchSpout tracesSpout = new FeederBatchSpout(Arrays.asList(
				TopologyBuilder.SESSION_ID_KEY,
				es.eucm.rage.realtime.topologies.TopologyBuilder.TRACE_KEY));

		TridentTopology topology = new TridentTopology();

		EsState.Factory partitionPersist = new EsState.Factory();

		// Test topology Builder configuration
		new MeanTopologyBuilder().build(topology,
				topology.newStream("testFileStream", tracesSpout),
				partitionPersist, null);

		Config conf = new Config();
		conf.put(AbstractAnalysis.SESSION_ID_FLUX_PARAM, NOW_DATE);
		conf.put(AbstractAnalysis.ZOOKEEPER_URL_FLUX_PARAM, ZOOKEEPER_URL);
		conf.put(AbstractAnalysis.ELASTICSEARCH_URL_FLUX_PARAM, ES_HOST);
		conf.put(AbstractAnalysis.SESSION_ID_FLUX_PARAM, NOW_DATE);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("realtime", conf, topology.build());

		CSVToMapTrace parser = new CSVToMapTrace(NOW_DATE);
		int totalTraces = 0;
		double totalSum = 0;
		String type = "level";
		String event = "completed";
		for (int i = 0; i < VERBS_FILES.length; ++i) {
			List<List<Object>> tuples = parser.getTuples("verbs/"
					+ VERBS_FILES[i] + ".csv");
			totalTraces += tuples.size();
			tracesSpout.feed(tuples);

			for (List<Object> tuple : tuples) {
				Map trace = (Map) tuple.get(1);
				double score = Double.valueOf((String) trace
						.get(TopologyBuilder.TridentTraceKeys.SCORE));
				type = trace.get(TopologyBuilder.TridentTraceKeys.TYPE)
						.toString();
				event = trace.get(TopologyBuilder.TridentTraceKeys.EVENT)
						.toString();
				totalSum += score;
			}
		}

		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
/*
		TransportClient client = partitionPersist
				.makeElasticsearchClient(Arrays.asList(InetAddress
						.getByName(ES_HOST)));

		String tracesIndex = ESUtils.getTracesIndex(NOW_DATE);

		Map meanState = client.prepareGet(tracesIndex, type, event)
				.setOperationThreaded(false).get().getSourceAsMap();

		assertEquals("Count completables " + totalTraces + ", current "
				+ meanState.get(AverageUpdater.COUNT_KEY), totalTraces,
				meanState.get(AverageUpdater.COUNT_KEY));

		assertEquals(
				"Total scores sum " + totalSum + ", current "
						+ meanState.get(AverageUpdater.SUM_KEY), totalSum,
				meanState.get(AverageUpdater.SUM_KEY));
				*/
	}
}
