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

