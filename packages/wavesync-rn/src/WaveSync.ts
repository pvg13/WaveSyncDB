import {NativeModules, NativeEventEmitter, Platform} from 'react-native';

const {WaveSync: NativeWaveSync} = NativeModules;

export interface WaveSyncOptions {
  passphrase?: string;
  relayServer?: string;
  rendezvousServer?: string;
  bootstrapPeer?: string;
  managedRelayAddr?: string;
  managedRelayApiKey?: string;
  ipv6?: boolean;
  syncIntervalSeconds?: number;
  keepAliveIntervalSeconds?: number;
}

export interface PeerInfo {
  peerId: string;
  address: string;
  dbVersion?: number;
  isBootstrap: boolean;
  isGroupMember: boolean;
  appId?: string;
}

export interface NetworkStatus {
  localPeerId: string;
  peerCount: number;
  groupPeerCount: number;
  relayStatus: string;
  natStatus: string;
  topic: string;
  rendezvousRegistered: boolean;
  pushRegistered: boolean;
  localDbVersion: number;
  registryReady: boolean;
  peers: PeerInfo[];
}

export interface ChangeNotification {
  table: string;
  kind: 'INSERT' | 'UPDATE' | 'DELETE';
  primaryKey: string;
  changedColumns?: string[];
}

export interface NetworkEvent {
  type:
    | 'PeerConnected'
    | 'PeerDisconnected'
    | 'PeerRejected'
    | 'PeerVerified'
    | 'PeerIdentityReceived'
    | 'RelayStatusChanged'
    | 'NatStatusChanged'
    | 'RendezvousStatusChanged'
    | 'PeerSynced'
    | 'EngineStarted'
    | 'EngineFailed';
  // PeerConnected
  peer?: PeerInfo;
  // PeerDisconnected, PeerRejected, PeerVerified, PeerIdentityReceived, PeerSynced
  peerId?: string;
  // PeerIdentityReceived
  appId?: string;
  // RelayStatusChanged, NatStatusChanged
  status?: string;
  // RendezvousStatusChanged
  registered?: boolean;
  // PeerSynced
  dbVersion?: number;
  // EngineFailed
  reason?: string;
}

const emitter = new NativeEventEmitter(NativeWaveSync);

export const WaveSync = {
  // -----------------------------------------------------------------------
  // Initialization
  // -----------------------------------------------------------------------

  initialize: (
    topic: string,
    options: WaveSyncOptions = {},
  ): Promise<boolean> => NativeWaveSync.initialize(topic, options),

  // -----------------------------------------------------------------------
  // SQL operations
  // -----------------------------------------------------------------------

  execute: (sql: string): Promise<number> => NativeWaveSync.execute(sql),

  query: async <T = Record<string, unknown>>(sql: string): Promise<T[]> => {
    const json = await NativeWaveSync.query(sql);
    return JSON.parse(json);
  },

  // -----------------------------------------------------------------------
  // Table registration
  // -----------------------------------------------------------------------

  registerSyncedTable: (
    tableName: string,
    pkColumn: string,
    columns: string[],
    createSql: string,
  ): Promise<boolean> =>
    NativeWaveSync.registerSyncedTable(tableName, pkColumn, columns, createSql),

  registryReady: (): Promise<boolean> => NativeWaveSync.registryReady(),

  // -----------------------------------------------------------------------
  // Network status
  // -----------------------------------------------------------------------

  networkStatus: (): Promise<NetworkStatus> => NativeWaveSync.networkStatus(),

  // -----------------------------------------------------------------------
  // Subscriptions
  // -----------------------------------------------------------------------

  /** Start bridging data-change notifications from the Rust engine to JS.
   *  Must be called before registryReady() to avoid missing catch-up sync events. */
  subscribeChanges: (): Promise<boolean> => NativeWaveSync.subscribeChanges(),

  /** Start bridging network events from the Rust engine to JS. */
  subscribeNetworkEvents: (): Promise<boolean> =>
    NativeWaveSync.subscribeNetworkEvents(),

  onChangeEvent: (callback: (notification: ChangeNotification) => void) =>
    emitter.addListener('WaveSyncChange', callback),

  onNetworkEvent: (callback: (event: NetworkEvent) => void) =>
    emitter.addListener('WaveSyncNetworkEvent', callback),

  // -----------------------------------------------------------------------
  // Lifecycle & sync control
  // -----------------------------------------------------------------------

  resume: (): Promise<boolean> => NativeWaveSync.resume(),

  networkTransition: (): Promise<boolean> => NativeWaveSync.networkTransition(),

  requestFullSync: (): Promise<boolean> => NativeWaveSync.requestFullSync(),

  isEngineAlive: (): Promise<boolean> => NativeWaveSync.isEngineAlive(),

  registerPushToken: (token: string): Promise<boolean> =>
    NativeWaveSync.registerPushToken(
      Platform.OS === 'ios' ? 'Apns' : 'Fcm',
      token,
    ),

  // -----------------------------------------------------------------------
  // Peer identity
  // -----------------------------------------------------------------------

  setPeerIdentity: (appId: string): Promise<boolean> =>
    NativeWaveSync.setPeerIdentity(appId),

  clearPeerIdentity: (): Promise<boolean> => NativeWaveSync.clearPeerIdentity(),

  // -----------------------------------------------------------------------
  // Database metadata
  // -----------------------------------------------------------------------

  nodeId: (): Promise<string> => NativeWaveSync.nodeId(),

  siteId: (): Promise<string> => NativeWaveSync.siteId(),

  /** Read the raw SyncConfig JSON for diagnostics. */
  readSyncConfig: (): Promise<string> => NativeWaveSync.readSyncConfig(),

  /** Shutdown the running engine and run a one-shot background sync.
   *  Returns { status: 'synced' | 'no_peers' | 'timed_out', peersSynced: number }.
   *  The engine is NOT restarted — call initialize() again after this. */
  testBackgroundSync: (
    timeoutSecs: number = 15,
  ): Promise<{status: string; peersSynced: number}> =>
    NativeWaveSync.testBackgroundSync(timeoutSecs),

  // -----------------------------------------------------------------------
  // Lifecycle
  // -----------------------------------------------------------------------

  shutdown: (): Promise<boolean> => NativeWaveSync.shutdown(),
};
