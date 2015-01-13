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
