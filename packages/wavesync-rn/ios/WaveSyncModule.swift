import Foundation

/// React Native native module for WaveSyncDB.
/// Mirrors the Android WaveSyncModule.kt — same method names, same JS API.
@objc(WaveSync)
class WaveSyncModule: RCTEventEmitter {

    private var ffi: WaveSyncFfi?
    private var hasListeners = false

    override init() {
        super.init()
    }

    @objc override static func requiresMainQueueSetup() -> Bool { false }

    override func supportedEvents() -> [String]! {
        ["WaveSyncChange", "WaveSyncNetworkEvent"]
    }

    override func startObserving() { hasListeners = true }
    override func stopObserving() { hasListeners = false }

    // MARK: - Initialization

    @objc func initialize(_ topic: String, options: NSDictionary, resolve: @escaping RCTPromiseResolveBlock, reject: @escaping RCTPromiseRejectBlock) {
        Task {
            do {
                let docsDir = NSSearchPathForDirectoriesInDomains(.documentDirectory, .userDomainMask, true).first!
                let dbPath = "\(docsDir)/wavesync.db"
                let dbUrl = "sqlite://\(dbPath)?mode=rwc"

                var builder = WaveSyncFfiBuilder(dbUrl: dbUrl, topic: topic)

                if let passphrase = options["passphrase"] as? String {
                    builder = builder.withPassphrase(passphrase: passphrase)
                }
                if let relay = options["relayServer"] as? String {
                    builder = builder.withRelayServer(addr: relay)
                }
                if let rendezvous = options["rendezvousServer"] as? String {
                    builder = builder.withRendezvousServer(addr: rendezvous)
                }
                if let bootstrap = options["bootstrapPeer"] as? String {
                    builder = builder.withBootstrapPeer(addr: bootstrap)
                }
                if let addr = options["managedRelayAddr"] as? String,
                   let key = options["managedRelayApiKey"] as? String {
                    builder = builder.managedRelay(addr: addr, apiKey: key)
                }
                if let ipv6 = options["ipv6"] as? Bool {
                    builder = builder.withIpv6(enabled: ipv6)
                }
                if let interval = options["syncIntervalSeconds"] as? NSNumber {
                    builder = builder.withSyncInterval(seconds: interval.uint64Value)
                }
                if let keepAlive = options["keepAliveIntervalSeconds"] as? NSNumber {
                    builder = builder.withKeepAliveInterval(seconds: keepAlive.uint64Value)
                }

                self.ffi = try await builder.build()
                resolve(true)
            } catch {
                reject("WAVESYNC_ERR", error.localizedDescription, error)
            }
        }
    }

    // MARK: - SQL Operations

    @objc func execute(_ sql: String, resolve: @escaping RCTPromiseResolveBlock, reject: @escaping RCTPromiseRejectBlock) {
        guard let engine = ffi else { return reject("NOT_INIT", "Call initialize first", nil) }
        Task {
            do {
                let rows = try await engine.execute(sql: sql)
                resolve(NSNumber(value: rows))
            } catch {
                reject("WAVESYNC_ERR", error.localizedDescription, error)
            }
        }
    }

    @objc func query(_ sql: String, resolve: @escaping RCTPromiseResolveBlock, reject: @escaping RCTPromiseRejectBlock) {
        guard let engine = ffi else { return reject("NOT_INIT", "Call initialize first", nil) }
        Task {
            do {
                let json = try await engine.query(sql: sql)
                resolve(json)
            } catch {
                reject("WAVESYNC_ERR", error.localizedDescription, error)
            }
        }
    }

    // MARK: - Table Registration

    @objc func registerSyncedTable(_ tableName: String, pkColumn: String, columns: [String], createSql: String, resolve: @escaping RCTPromiseResolveBlock, reject: @escaping RCTPromiseRejectBlock) {
        guard let engine = ffi else { return reject("NOT_INIT", "Call initialize first", nil) }
        Task {
            do {
                try await engine.registerSyncedTable(tableName: tableName, pkColumn: pkColumn, columns: columns, createSql: createSql)
                resolve(true)
            } catch {
                reject("WAVESYNC_ERR", error.localizedDescription, error)
            }
        }
    }

    @objc func registryReady(_ resolve: @escaping RCTPromiseResolveBlock, reject: @escaping RCTPromiseRejectBlock) {
        guard let engine = ffi else { return reject("NOT_INIT", "Call initialize first", nil) }
        engine.registryReady()
        resolve(true)
    }

    // MARK: - Network Status

    @objc func networkStatus(_ resolve: @escaping RCTPromiseResolveBlock, reject: @escaping RCTPromiseRejectBlock) {
        guard let engine = ffi else { return reject("NOT_INIT", "Call initialize first", nil) }
        let status = engine.networkStatus()
        let peers = status.peers.map { peer -> [String: Any] in
            var map: [String: Any] = [
                "peerId": peer.peerId,
                "address": peer.address,
                "isBootstrap": peer.isBootstrap,
                "isGroupMember": peer.isGroupMember,
            ]
            if let v = peer.dbVersion { map["dbVersion"] = NSNumber(value: v) }
            if let id = peer.appId { map["appId"] = id }
            return map
        }
        let map: [String: Any] = [
            "localPeerId": status.localPeerId,
            "peerCount": status.peerCount,
            "groupPeerCount": status.groupPeerCount,
            "relayStatus": String(describing: status.relayStatus).uppercased(),
            "natStatus": String(describing: status.natStatus).uppercased(),
            "topic": status.topic,
            "rendezvousRegistered": status.rendezvousRegistered,
            "pushRegistered": status.pushRegistered,
            "localDbVersion": NSNumber(value: status.localDbVersion),
            "registryReady": status.registryReady,
            "peers": peers,
        ]
        resolve(map)
    }

    // MARK: - Subscriptions

    @objc func subscribeChanges(_ resolve: @escaping RCTPromiseResolveBlock, reject: @escaping RCTPromiseRejectBlock) {
        guard let engine = ffi else { return reject("NOT_INIT", "Call initialize first", nil) }
        engine.subscribeChanges(listener: ChangeListenerImpl(module: self))
        resolve(true)
    }

    @objc func subscribeNetworkEvents(_ resolve: @escaping RCTPromiseResolveBlock, reject: @escaping RCTPromiseRejectBlock) {
        guard let engine = ffi else { return reject("NOT_INIT", "Call initialize first", nil) }
        engine.subscribeNetworkEvents(listener: NetworkEventListenerImpl(module: self))
        resolve(true)
    }

    // MARK: - Lifecycle & Sync Control

    @objc func resume(_ resolve: @escaping RCTPromiseResolveBlock, reject: @escaping RCTPromiseRejectBlock) {
        guard let engine = ffi else { return reject("NOT_INIT", "Call initialize first", nil) }
        engine.resume()
        resolve(true)
    }

    @objc func networkTransition(_ resolve: @escaping RCTPromiseResolveBlock, reject: @escaping RCTPromiseRejectBlock) {
        guard let engine = ffi else { return reject("NOT_INIT", "Call initialize first", nil) }
        engine.networkTransition()
        resolve(true)
    }

    @objc func requestFullSync(_ resolve: @escaping RCTPromiseResolveBlock, reject: @escaping RCTPromiseRejectBlock) {
        guard let engine = ffi else { return reject("NOT_INIT", "Call initialize first", nil) }
        engine.requestFullSync()
        resolve(true)
    }

    @objc func isEngineAlive(_ resolve: @escaping RCTPromiseResolveBlock, reject: @escaping RCTPromiseRejectBlock) {
        guard let engine = ffi else { return reject("NOT_INIT", "Call initialize first", nil) }
        resolve(engine.isEngineAlive())
    }

    @objc func registerPushToken(_ platform: String, token: String, resolve: @escaping RCTPromiseResolveBlock, reject: @escaping RCTPromiseRejectBlock) {
        guard let engine = ffi else { return reject("NOT_INIT", "Call initialize first", nil) }
        engine.registerPushToken(platform: platform, token: token)
        resolve(true)
    }

    // MARK: - Peer Identity

    @objc func setPeerIdentity(_ appId: String, resolve: @escaping RCTPromiseResolveBlock, reject: @escaping RCTPromiseRejectBlock) {
        guard let engine = ffi else { return reject("NOT_INIT", "Call initialize first", nil) }
        engine.setPeerIdentity(appId: appId)
        resolve(true)
    }

    @objc func clearPeerIdentity(_ resolve: @escaping RCTPromiseResolveBlock, reject: @escaping RCTPromiseRejectBlock) {
        guard let engine = ffi else { return reject("NOT_INIT", "Call initialize first", nil) }
        engine.clearPeerIdentity()
        resolve(true)
    }

    // MARK: - Database Metadata

    @objc func nodeId(_ resolve: @escaping RCTPromiseResolveBlock, reject: @escaping RCTPromiseRejectBlock) {
        guard let engine = ffi else { return reject("NOT_INIT", "Call initialize first", nil) }
        resolve(engine.nodeId())
    }

    @objc func siteId(_ resolve: @escaping RCTPromiseResolveBlock, reject: @escaping RCTPromiseRejectBlock) {
        guard let engine = ffi else { return reject("NOT_INIT", "Call initialize first", nil) }
        resolve(engine.siteId())
    }

    @objc func readSyncConfig(_ resolve: @escaping RCTPromiseResolveBlock, reject: @escaping RCTPromiseRejectBlock) {
        guard let engine = ffi else { return reject("NOT_INIT", "Call initialize first", nil) }
        do {
            resolve(try engine.readSyncConfig())
        } catch {
            reject("WAVESYNC_ERR", error.localizedDescription, error)
        }
    }

    // MARK: - Shutdown

    @objc func shutdown(_ resolve: @escaping RCTPromiseResolveBlock, reject: @escaping RCTPromiseRejectBlock) {
        guard let engine = ffi else { return resolve(true) }
        Task {
            do {
                try await engine.shutdown()
                self.ffi = nil
                resolve(true)
            } catch {
                reject("WAVESYNC_ERR", error.localizedDescription, error)
            }
        }
    }

    // MARK: - Event Emitter Stubs

    @objc func addListener(_ eventName: String) { /* required by NativeEventEmitter */ }
    @objc func removeListeners(_ count: Double) { /* required by NativeEventEmitter */ }

    // MARK: - Internal: Send events to JS

    func emitChange(_ notification: FfiChangeNotification) {
        guard hasListeners else { return }
        var params: [String: Any] = [
            "table": notification.table,
            "kind": String(describing: notification.kind).uppercased(),
            "primaryKey": notification.primaryKey,
        ]
        if let cols = notification.changedColumns {
            params["changedColumns"] = cols
        }
        sendEvent(withName: "WaveSyncChange", body: params)
    }

    func emitNetworkEvent(_ event: FfiNetworkEvent) {
        guard hasListeners else { return }
        var params: [String: Any] = [:]
        switch event {
        case .peerConnected(let peer):
            params["type"] = "PeerConnected"
            params["peer"] = peerInfoToDict(peer)
        case .peerDisconnected(let peerId):
            params["type"] = "PeerDisconnected"
            params["peerId"] = peerId
        case .peerRejected(let peerId):
            params["type"] = "PeerRejected"
            params["peerId"] = peerId
        case .peerVerified(let peerId):
            params["type"] = "PeerVerified"
            params["peerId"] = peerId
        case .peerIdentityReceived(let peerId, let appId):
            params["type"] = "PeerIdentityReceived"
            params["peerId"] = peerId
            params["appId"] = appId
        case .relayStatusChanged(let status):
            params["type"] = "RelayStatusChanged"
            params["status"] = String(describing: status).uppercased()
        case .natStatusChanged(let status):
            params["type"] = "NatStatusChanged"
            params["status"] = String(describing: status).uppercased()
        case .rendezvousStatusChanged(let registered):
            params["type"] = "RendezvousStatusChanged"
            params["registered"] = registered
        case .peerSynced(let peerId, let dbVersion):
            params["type"] = "PeerSynced"
            params["peerId"] = peerId
            params["dbVersion"] = NSNumber(value: dbVersion)
        case .engineStarted:
            params["type"] = "EngineStarted"
        case .engineFailed(let reason):
            params["type"] = "EngineFailed"
            params["reason"] = reason
        }
        sendEvent(withName: "WaveSyncNetworkEvent", body: params)
    }

    private func peerInfoToDict(_ peer: FfiPeerInfo) -> [String: Any] {
        var map: [String: Any] = [
            "peerId": peer.peerId,
            "address": peer.address,
            "isBootstrap": peer.isBootstrap,
            "isGroupMember": peer.isGroupMember,
        ]
        if let v = peer.dbVersion { map["dbVersion"] = NSNumber(value: v) }
        if let id = peer.appId { map["appId"] = id }
        return map
    }
}

// MARK: - Callback Implementations

class ChangeListenerImpl: ChangeListener {
    private weak var module: WaveSyncModule?

    init(module: WaveSyncModule) {
        self.module = module
    }

    func onChange(notification: FfiChangeNotification) {
        module?.emitChange(notification)
    }
}

class NetworkEventListenerImpl: NetworkEventListener {
    private weak var module: WaveSyncModule?

    init(module: WaveSyncModule) {
        self.module = module
    }

    func onEvent(event: FfiNetworkEvent) {
        module?.emitNetworkEvent(event)
    }
}
