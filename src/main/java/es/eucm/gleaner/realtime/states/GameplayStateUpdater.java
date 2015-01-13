package es.eucm.gleaner.realtime.states;

import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.state.StateUpdater;
import storm.trident.tuple.TridentTuple;

import java.util.List;
import java.util.Map;

/**
 * Created by angel on 9/01/15.
 */
public class GameplayStateUpdater implements StateUpdater<GameplayState> {

	@Override
	public void updateState(GameplayState state, List<TridentTuple> tuples,
			TridentCollector collector) {
		for (TridentTuple tuple : tuples) {
			String versionId = tuple.getStringByField("versionId");
			String gameplayId = tuple.getStringByField("gameplayId");
			String property = tuple.getStringByField("p");
			Object value = tuple.getValueByField("v");
			state.setProperty(versionId, gameplayId, property, value);
		}
	}

	@Override
	public void prepare(Map conf, TridentOperationContext context) {

	}

	@Override
	public void cleanup() {

	}
}
