import React, {useCallback, useEffect, useRef, useState} from 'react';
import {
  View,
  Text,
  TextInput,
  TouchableOpacity,
  FlatList,
  StyleSheet,
  StatusBar,
  ActivityIndicator,
} from 'react-native';
import {SafeAreaProvider, SafeAreaView} from 'react-native-safe-area-context';
import withObservables from '@nozbe/with-observables';
import {database, adapter} from './src/db/database';
import {Task} from './src/db/Task';
import {WaveSync, initWaveSyncFCM} from '@wavesync/react-native';
import type {NetworkStatus, NetworkEvent} from '@wavesync/react-native';

// ─── TaskRow ────────────────────────────────────────────────────────────────

const TaskRowBase = ({task}: {task: Task}) => (
  <View style={styles.row}>
    <TouchableOpacity
      style={[styles.checkbox, task.done && styles.checkboxDone]}
      onPress={() =>
        database.write(() =>
          task.update(t => {
            t.done = !t.done;
          }),
        )
      }>
      <Text style={styles.checkboxText}>{task.done ? '✓' : ' '}</Text>
    </TouchableOpacity>
    <Text style={[styles.taskTitle, task.done && styles.taskDone]}>
      {task.title}
    </Text>
    <TouchableOpacity
      onPress={() => database.write(() => task.destroyPermanently())}>
      <Text style={styles.deleteBtn}>✕</Text>
    </TouchableOpacity>
  </View>
);

// ─── TaskList ────────────────────────────────────────────────────────────────

const TaskListBase = ({tasks}: {tasks: Task[]}) => {
  if (tasks.length === 0) {
    return (
      <View style={styles.empty}>
        <Text style={styles.emptyText}>No tasks yet.</Text>
        <Text style={styles.emptyHint}>
          Add one above or tap Simulate Remote.
        </Text>
      </View>
    );
  }
  return (
    <FlatList
      data={tasks}
      keyExtractor={t => t.id}
      renderItem={({item}) => <TaskRowBase task={item} />}
      contentContainerStyle={{paddingBottom: 20}}
    />
  );
};

const enhance = withObservables([], () => ({
  tasks: database
    .get<Task>('tasks')
    .query()
    .observeWithColumns(['done', 'title']),
}));

const TaskList = enhance(TaskListBase);

// ─── NetworkPanel ────────────────────────────────────────────────────────────

function useNetworkStatus(): NetworkStatus | null {
  const [status, setStatus] = useState<NetworkStatus | null>(null);
  const mounted = useRef(true);

  const refresh = useCallback(async () => {
    try {
      const s = await WaveSync.networkStatus();
      if (mounted.current) setStatus(s);
    } catch {}
  }, []);

  useEffect(() => {
    mounted.current = true;
    refresh();
    const sub = WaveSync.onNetworkEvent(() => refresh());
    return () => {
      mounted.current = false;
      sub.remove();
    };
  }, [refresh]);

  return status;
}

const StatusPill = ({label, value, color}: {label: string; value: string; color?: string}) => (
  <View style={styles.pill}>
    <Text style={styles.pillLabel}>{label}</Text>
    <Text style={[styles.pillValue, color ? {color} : null]}>{value}</Text>
  </View>
);

const NetworkPanel = () => {
  const status = useNetworkStatus();
  const [events, setEvents] = useState<string[]>([]);
  const [expanded, setExpanded] = useState(false);

  useEffect(() => {
    const sub = WaveSync.onNetworkEvent((event: NetworkEvent) => {
      const ts = new Date().toLocaleTimeString([], {hour: '2-digit', minute: '2-digit', second: '2-digit'});
      let msg = event.type;
      if (event.type === 'PeerConnected') msg = `+ ${event.peer?.peerId.slice(-8)}`;
      else if (event.type === 'PeerDisconnected') msg = `- ${event.peerId?.slice(-8)}`;
      else if (event.type === 'PeerSynced') msg = `synced ${event.peerId?.slice(-8)} v${event.dbVersion}`;
      else if (event.type === 'RelayStatusChanged') msg = `relay ${event.status}`;
      else if (event.type === 'NatStatusChanged') msg = `NAT ${event.status}`;
      else if (event.type === 'PeerVerified') msg = `verified ${event.peerId?.slice(-8)}`;
      setEvents(prev => [`${ts}  ${msg}`, ...prev].slice(0, 30));
    });
    return () => sub.remove();
  }, []);

  if (!status) return null;

  const relayColor =
    status.relayStatus === 'LISTENING' ? '#5c7a5c' :
    status.relayStatus === 'CONNECTED' ? '#a08040' : '#aaa';

  const natColor =
    status.natStatus === 'PUBLIC' ? '#5c7a5c' :
    status.natStatus === 'PRIVATE' ? '#a08040' : '#aaa';

  return (
    <View style={styles.netPanel}>
      <TouchableOpacity
        style={styles.netHeader}
        onPress={() => setExpanded(e => !e)}
        activeOpacity={0.7}>
        <View style={styles.netHeaderLeft}>
          <View style={[styles.dot, {backgroundColor: status.groupPeerCount > 0 ? '#5c7a5c' : '#aaa'}]} />
          <Text style={styles.netHeaderText}>
            {status.groupPeerCount} group / {status.peerCount} total peers
          </Text>
        </View>
        <Text style={styles.chevron}>{expanded ? '\u25B2' : '\u25BC'}</Text>
      </TouchableOpacity>

      {expanded && (
        <View style={styles.netBody}>
          <View style={styles.pillRow}>
            <StatusPill label="Relay" value={status.relayStatus} color={relayColor} />
            <StatusPill label="NAT" value={status.natStatus} color={natColor} />
            <StatusPill label="DB ver" value={String(status.localDbVersion)} />
          </View>

          <View style={styles.pillRow}>
            <StatusPill
              label="Rendezvous"
              value={status.rendezvousRegistered ? 'yes' : 'no'}
              color={status.rendezvousRegistered ? '#5c7a5c' : '#aaa'} />
            <StatusPill
              label="Push"
              value={status.pushRegistered ? 'yes' : 'no'}
              color={status.pushRegistered ? '#5c7a5c' : '#aaa'} />
          </View>

          <View style={styles.netMeta}>
            <Text style={styles.netMetaText}>Peer ID: {status.localPeerId.slice(0, 20)}...</Text>
            <Text style={styles.netMetaText}>Topic: {status.topic.slice(0, 24)}{status.topic.length > 24 ? '...' : ''}</Text>
          </View>

          {status.peers.length > 0 && (
            <View style={styles.peerSection}>
              <Text style={styles.peerSectionTitle}>Connected Peers</Text>
              {status.peers.map(p => (
                <View key={p.peerId} style={styles.peerRow}>
                  <View style={styles.peerRowTop}>
                    <Text style={styles.peerId}>...{p.peerId.slice(-12)}</Text>
                    <View style={styles.badges}>
                      {p.isGroupMember && <Text style={styles.badgeGroup}>group</Text>}
                      {p.isBootstrap && <Text style={styles.badgeBoot}>boot</Text>}
                    </View>
                  </View>
                  <Text style={styles.peerAddr} numberOfLines={1}>
                    {p.address}
                  </Text>
                  {p.dbVersion != null && (
                    <Text style={styles.peerMeta}>db_version: {p.dbVersion}</Text>
                  )}
                </View>
              ))}
            </View>
          )}

          {events.length > 0 && (
            <View style={styles.eventSection}>
              <Text style={styles.peerSectionTitle}>Events</Text>
              {events.map((line, i) => (
                <Text key={i} style={styles.eventLine}>{line}</Text>
              ))}
            </View>
          )}
        </View>
      )}
    </View>
  );
};

// ─── App ─────────────────────────────────────────────────────────────────────

export default function App() {
  const [input, setInput] = useState('');
  const [ready, setReady] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    adapter.initializingPromise
      .then(async () => {
        // Subscribe to network events before marking ready so the panel
        // doesn't miss early events.
        await WaveSync.subscribeNetworkEvents();
        setReady(true);
        // Register FCM token for push wakeup (no-op if Firebase not configured)
        await initWaveSyncFCM();
      })
      .catch(e => setError(String(e?.message ?? e)));
  }, []);

  const addTask = async () => {
    const title = input.trim();
    if (!title) return;
    setInput('');
    await database.write(async () => {
      await database.get<Task>('tasks').create(t => {
        t.title = title;
        t.done = false;
      });
    });
  };

  const simulateRemote = async () => {
    const id = `remote-${Date.now()}`;
    await WaveSync.execute(
      `INSERT INTO tasks (id, _status, _changed, title, done, created_at) VALUES ('${id}', '', '', 'Remote task (simulated)', 0, ${Date.now()})`,
    );
  };

  if (error) {
    return (
      <SafeAreaProvider>
        <SafeAreaView style={styles.center}>
          <Text style={styles.errorText}>{error}</Text>
        </SafeAreaView>
      </SafeAreaProvider>
    );
  }

  if (!ready) {
    return (
      <SafeAreaProvider>
        <SafeAreaView style={styles.center}>
          <ActivityIndicator color="#5c7a5c" />
          <Text style={styles.loadingText}>Starting WaveSyncDB…</Text>
        </SafeAreaView>
      </SafeAreaProvider>
    );
  }

  return (
    <SafeAreaProvider>
      <SafeAreaView style={styles.container} edges={['top', 'bottom', 'left', 'right']}>
        <StatusBar barStyle="dark-content" backgroundColor="#f5f5f0" />

        <View style={styles.header}>
          <Text style={styles.headerTitle}>WaveSync Demo</Text>
        </View>

        <NetworkPanel />

        <View style={styles.inputRow}>
          <TextInput
            style={styles.input}
            value={input}
            onChangeText={setInput}
            placeholder="New task…"
            placeholderTextColor="#aaa"
            onSubmitEditing={addTask}
            returnKeyType="done"
          />
          <TouchableOpacity style={styles.addBtn} onPress={addTask}>
            <Text style={styles.addBtnText}>Add</Text>
          </TouchableOpacity>
        </View>

        <TaskList />

        <TouchableOpacity style={styles.simulateBtn} onPress={simulateRemote}>
          <Text style={styles.simulateBtnText}>
            ⚡ Simulate Remote Insert
          </Text>
        </TouchableOpacity>
      </SafeAreaView>
    </SafeAreaProvider>
  );
}

// ─── Styles ──────────────────────────────────────────────────────────────────

const styles = StyleSheet.create({
  container: {flex: 1, backgroundColor: '#f5f5f0'},
  center: {
    flex: 1,
    alignItems: 'center',
    justifyContent: 'center',
    backgroundColor: '#f5f5f0',
  },
  loadingText: {marginTop: 12, color: '#888', fontSize: 14},
  errorText: {color: '#c0392b', fontSize: 16},

  header: {paddingHorizontal: 20, paddingTop: 16, paddingBottom: 8},
  headerTitle: {fontSize: 24, fontWeight: '700', color: '#2c3e2d'},

  // ── Network panel ──
  netPanel: {
    marginHorizontal: 16,
    marginBottom: 8,
    backgroundColor: '#fff',
    borderRadius: 10,
    borderWidth: 1,
    borderColor: '#e0e0d8',
    overflow: 'hidden',
  },
  netHeader: {
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'space-between',
    paddingHorizontal: 14,
    paddingVertical: 10,
  },
  netHeaderLeft: {flexDirection: 'row', alignItems: 'center'},
  netHeaderText: {fontSize: 13, color: '#666'},
  chevron: {fontSize: 10, color: '#aaa'},
  dot: {width: 8, height: 8, borderRadius: 4, marginRight: 6},
  netBody: {paddingHorizontal: 14, paddingBottom: 12},
  pillRow: {flexDirection: 'row', gap: 6, marginBottom: 6},
  pill: {
    flexDirection: 'row',
    alignItems: 'center',
    backgroundColor: '#f5f5f0',
    borderRadius: 6,
    paddingHorizontal: 8,
    paddingVertical: 4,
    gap: 4,
  },
  pillLabel: {fontSize: 11, color: '#999'},
  pillValue: {fontSize: 11, fontWeight: '600', color: '#555'},
  netMeta: {marginTop: 2, marginBottom: 6},
  netMetaText: {fontSize: 11, color: '#bbb', fontFamily: 'monospace'},
  peerSection: {marginTop: 4},
  peerSectionTitle: {
    fontSize: 11,
    fontWeight: '600',
    color: '#aaa',
    textTransform: 'uppercase',
    letterSpacing: 0.5,
    marginBottom: 4,
  },
  peerRow: {
    backgroundColor: '#fafaf5',
    borderRadius: 6,
    padding: 8,
    marginBottom: 4,
  },
  peerRowTop: {flexDirection: 'row', justifyContent: 'space-between', alignItems: 'center'},
  peerId: {fontSize: 11, fontFamily: 'monospace', color: '#5c7a5c'},
  badges: {flexDirection: 'row', gap: 4},
  badgeGroup: {
    fontSize: 9,
    color: '#5c7a5c',
    backgroundColor: '#e8f0e8',
    paddingHorizontal: 4,
    paddingVertical: 1,
    borderRadius: 3,
    overflow: 'hidden',
  },
  badgeBoot: {
    fontSize: 9,
    color: '#6080a0',
    backgroundColor: '#e8eef5',
    paddingHorizontal: 4,
    paddingVertical: 1,
    borderRadius: 3,
    overflow: 'hidden',
  },
  peerAddr: {fontSize: 10, color: '#bbb', fontFamily: 'monospace', marginTop: 2},
  peerMeta: {fontSize: 10, color: '#ccc', fontFamily: 'monospace'},
  eventSection: {marginTop: 8},
  eventLine: {fontSize: 10, color: '#bbb', fontFamily: 'monospace', lineHeight: 15},

  inputRow: {flexDirection: 'row', margin: 16, gap: 8},
  input: {
    flex: 1,
    backgroundColor: '#fff',
    borderRadius: 10,
    paddingHorizontal: 14,
    paddingVertical: 10,
    fontSize: 16,
    color: '#2c3e2d',
    borderWidth: 1,
    borderColor: '#e0e0d8',
  },
  addBtn: {
    backgroundColor: '#5c7a5c',
    borderRadius: 10,
    paddingHorizontal: 18,
    justifyContent: 'center',
  },
  addBtnText: {color: '#fff', fontWeight: '600', fontSize: 16},

  row: {
    flexDirection: 'row',
    alignItems: 'center',
    paddingHorizontal: 16,
    paddingVertical: 12,
    backgroundColor: '#fff',
    marginHorizontal: 16,
    marginBottom: 4,
    borderRadius: 10,
  },
  checkbox: {
    width: 24,
    height: 24,
    borderRadius: 6,
    borderWidth: 2,
    borderColor: '#5c7a5c',
    alignItems: 'center',
    justifyContent: 'center',
    marginRight: 12,
  },
  checkboxDone: {backgroundColor: '#5c7a5c'},
  checkboxText: {color: '#fff', fontWeight: '700', fontSize: 14},
  taskTitle: {flex: 1, fontSize: 16, color: '#2c3e2d'},
  taskDone: {textDecorationLine: 'line-through', color: '#aaa'},
  deleteBtn: {color: '#ccc', fontSize: 18, paddingLeft: 8},

  empty: {
    flex: 1,
    alignItems: 'center',
    justifyContent: 'center',
    padding: 40,
  },
  emptyText: {fontSize: 18, color: '#aaa', marginBottom: 8},
  emptyHint: {fontSize: 14, color: '#ccc', textAlign: 'center'},

  simulateBtn: {
    margin: 16,
    padding: 14,
    backgroundColor: '#fff',
    borderRadius: 10,
    alignItems: 'center',
    borderWidth: 1,
    borderColor: '#e0e0d8',
  },
  simulateBtnText: {color: '#5c7a5c', fontWeight: '600', fontSize: 15},
});
