package es.eucm.gleaner.realtime.states;

import storm.trident.state.OpaqueValue;
import storm.trident.state.map.IBackingMap;
import storm.trident.state.map.MapState;
import storm.trident.state.map.OpaqueMap;

import java.util.ArrayList;
import java.util.List;

public abstract class GameplayState implements MapState {

	private MapState mapState;

	public GameplayState() {
		mapState = (OpaqueMap) OpaqueMap.build(new BackingMap());
	}

	public abstract void setProperty(String versionId, String gameplayId,
			String key, Object value);

	public abstract void setOpaqueValue(String versionId, String gameplayId,
			List<Object> key, OpaqueValue value);

	public abstract OpaqueValue getOpaqueValue(String versionId,
			String gameplayId, List<Object> key);

	@Override
	public List multiUpdate(List keys, List list) {
		return mapState.multiUpdate(keys, list);
	}

	@Override
	public void multiPut(List keys, List vals) {
		mapState.multiPut(keys, vals);
	}

	@Override
	public List multiGet(List keys) {
		return mapState.multiGet(keys);
	}

	@Override
	public void beginCommit(Long txid) {
		mapState.beginCommit(txid);
	}

	@Override
	public void commit(Long txid) {
		mapState.commit(txid);
	}

	public class BackingMap implements IBackingMap<OpaqueValue> {
		@Override
		public List<OpaqueValue> multiGet(List<List<Object>> keys) {
			ArrayList<OpaqueValue> values = new ArrayList<OpaqueValue>();
			for (List<Object> key : keys) {
				String versionId = (String) key.get(0);
				String gameplayId = (String) key.get(1);
				values.add(getOpaqueValue(versionId, gameplayId,
						key.subList(2, key.size())));
			}
			return values;
		}

		@Override
		public void multiPut(List<List<Object>> keys, List<OpaqueValue> vals) {
			int j = 0;
			for (List<Object> key : keys) {
				String versionId = (String) key.get(0);
				String gameplayId = (String) key.get(1);
				setOpaqueValue(versionId, gameplayId,
						key.subList(2, key.size()), vals.get(j++));
			}
		}
	}

}
