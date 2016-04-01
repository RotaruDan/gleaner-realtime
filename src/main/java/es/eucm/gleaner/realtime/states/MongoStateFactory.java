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
package es.eucm.gleaner.realtime.states;

import backtype.storm.task.IMetricsContext;
import es.eucm.gleaner.realtime.utils.DBUtils;
import storm.trident.state.State;
import storm.trident.state.StateFactory;

import java.util.Map;

public class MongoStateFactory implements StateFactory {

	@Override
	public State makeState(Map conf, IMetricsContext metrics,
			int partitionIndex, int numPartitions) {
		return new MongoGameplayState(DBUtils.getMongoDB(conf));
	}

}
