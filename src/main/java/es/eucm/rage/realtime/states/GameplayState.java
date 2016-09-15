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
package es.eucm.rage.realtime.states;

import storm.trident.state.OpaqueValue;
import storm.trident.state.map.IBackingMap;
import storm.trident.state.map.MapState;
import storm.trident.state.map.OpaqueMap;

import java.util.ArrayList;
import java.util.List;

/**
 * Storm {@link MapState} designed to keep the gameplay state of each player
 * individually
 */
public abstract class GameplayState implements MapState {

	// Opaque values keys
	protected static final String TRANSACTION_ID = "txid";
	protected static final String CURRENT_VALUE_ID = "curr";
	protected static final String PREVIOUS_VALUE_ID = "prev";
	protected static final String VALUE_KEY = "value";
	protected static final String KEY_KEY = "key";

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