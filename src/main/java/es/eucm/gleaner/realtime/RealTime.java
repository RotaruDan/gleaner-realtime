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
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import es.eucm.gleaner.realtime.states.MongoStateFactory;
import es.eucm.gleaner.realtime.topologies.KafkaTopology;
import es.eucm.gleaner.realtime.utils.DBUtils;

import java.util.Map;

public class RealTime {

	private static StormTopology buildTopology(Map conf, String sessionId, String zookeeperUrl) {
		DBUtils.startRealtime(DBUtils.getMongoDB(conf), sessionId);

		KafkaTopology kafkaTopology = new KafkaTopology(sessionId);
		kafkaTopology.prepare(new MongoStateFactory(), zookeeperUrl);
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
		String zookeeperUrl = args[2];
        setUpConfig(conf, args[1]);

		if (args.length == 4 && "debug".equals(args[3])) {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(sessionId, conf,
					buildTopology(conf, sessionId, zookeeperUrl));
		} else {
			try {
				System.out.println("Starting analysis of session " + sessionId);
				StormSubmitter.submitTopology(sessionId, conf,
						buildTopology(conf, sessionId, zookeeperUrl));
			} catch (AlreadyAliveException e) {
				e.printStackTrace();
			} catch (InvalidTopologyException e) {
				e.printStackTrace();
			}
		}
	}
}

