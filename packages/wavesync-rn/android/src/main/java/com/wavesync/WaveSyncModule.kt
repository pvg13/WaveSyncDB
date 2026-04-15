package com.wavesync

import com.facebook.react.bridge.*
import com.facebook.react.modules.core.DeviceEventManagerModule
import kotlinx.coroutines.*
import uniffi.wavesyncdb_ffi.*

class WaveSyncModule(reactContext: ReactApplicationContext) :
    ReactContextBaseJavaModule(reactContext) {

    override fun getName() = "WaveSync"

    private var ffi: WaveSyncFfi? = null
    private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())

    // -----------------------------------------------------------------------
    // Initialization
    // -----------------------------------------------------------------------

    @ReactMethod
    fun initialize(topic: String, options: ReadableMap, promise: Promise) {
        scope.launch {
            try {
                val dbPath = "${reactApplicationContext.filesDir.absolutePath}/wavesync.db"
                val dbUrl = "sqlite://$dbPath?mode=rwc"

                var builder = WaveSyncFfiBuilder(dbUrl, topic)

                options.getString("passphrase")?.let {
                    builder = builder.withPassphrase(it)
                }
                options.getString("relayServer")?.let {
                    builder = builder.withRelayServer(it)
                }
                options.getString("rendezvousServer")?.let {
                    builder = builder.withRendezvousServer(it)
                }
                options.getString("bootstrapPeer")?.let {
                    builder = builder.withBootstrapPeer(it)
                }
                options.getString("managedRelayAddr")?.let { addr ->
                    options.getString("managedRelayApiKey")?.let { key ->
                        builder = builder.managedRelay(addr, key)
                    }
                }
                if (options.hasKey("ipv6")) {
                    builder = builder.withIpv6(options.getBoolean("ipv6"))
                }
                if (options.hasKey("syncIntervalSeconds")) {
                    builder = builder.withSyncInterval(options.getDouble("syncIntervalSeconds").toLong().toULong())
                }
                if (options.hasKey("keepAliveIntervalSeconds")) {
                    builder = builder.withKeepAliveInterval(options.getDouble("keepAliveIntervalSeconds").toLong().toULong())
                }

                ffi = builder.build()
                promise.resolve(true)
            } catch (e: WaveSyncException) {
                promise.reject("WAVESYNC_ERR", e.message, e)
            } catch (e: Exception) {
                promise.reject("WAVESYNC_INIT_ERR", e.message, e)
            }
        }
    }

    // -----------------------------------------------------------------------
    // SQL operations
    // -----------------------------------------------------------------------

    @ReactMethod
    fun execute(sql: String, promise: Promise) {
        val engine = ffi ?: return promise.reject("NOT_INIT", "Call initialize first")
        scope.launch {
            try {
                val rowsAffected = engine.execute(sql)
                promise.resolve(rowsAffected.toLong().toDouble())
            } catch (e: WaveSyncException) {
                promise.reject("WAVESYNC_ERR", e.message, e)
            }
        }
    }

    @ReactMethod
    fun query(sql: String, promise: Promise) {
        val engine = ffi ?: return promise.reject("NOT_INIT", "Call initialize first")
        scope.launch {
            try {
                val jsonStr = engine.query(sql)
                promise.resolve(jsonStr)
            } catch (e: WaveSyncException) {
                promise.reject("WAVESYNC_ERR", e.message, e)
            }
        }
    }

    // -----------------------------------------------------------------------
    // Table registration
    // -----------------------------------------------------------------------

    @ReactMethod
    fun registerSyncedTable(
        tableName: String,
        pkColumn: String,
        columns: ReadableArray,
        createSql: String,
        promise: Promise
    ) {
        val engine = ffi ?: return promise.reject("NOT_INIT", "Call initialize first")
        scope.launch {
            try {
                val colList = (0 until columns.size()).map { columns.getString(it)!! }
                engine.registerSyncedTable(tableName, pkColumn, colList, createSql)
                promise.resolve(true)
            } catch (e: WaveSyncException) {
                promise.reject("WAVESYNC_ERR", e.message, e)
            }
        }
    }

    @ReactMethod
    fun registryReady(promise: Promise) {
        val engine = ffi ?: return promise.reject("NOT_INIT", "Call initialize first")
        try {
            engine.registryReady()
            promise.resolve(true)
        } catch (e: Exception) {
            promise.reject("WAVESYNC_ERR", e.message, e)
        }
    }

    // -----------------------------------------------------------------------
    // Network status
    // -----------------------------------------------------------------------

    @ReactMethod
    fun networkStatus(promise: Promise) {
        val engine = ffi ?: return promise.reject("NOT_INIT", "Call initialize first")
        try {
            val status = engine.networkStatus()
            val map = Arguments.createMap().apply {
                putString("localPeerId", status.localPeerId)
                putInt("peerCount", status.peerCount.toInt())
                putInt("groupPeerCount", status.groupPeerCount.toInt())
                putString("relayStatus", status.relayStatus.name)
                putString("natStatus", status.natStatus.name)
                putString("topic", status.topic)
                putBoolean("rendezvousRegistered", status.rendezvousRegistered)
                putBoolean("pushRegistered", status.pushRegistered)
                putDouble("localDbVersion", status.localDbVersion.toLong().toDouble())
                putBoolean("registryReady", status.registryReady)

                val peersArray = Arguments.createArray()
                for (peer in status.peers) {
                    peersArray.pushMap(peerInfoToMap(peer))
                }
                putArray("peers", peersArray)
            }
            promise.resolve(map)
        } catch (e: Exception) {
            promise.reject("WAVESYNC_ERR", e.message, e)
        }
    }

    // -----------------------------------------------------------------------
    // Subscriptions
    // -----------------------------------------------------------------------

    @ReactMethod
    fun subscribeChanges(promise: Promise) {
        val engine = ffi ?: return promise.reject("NOT_INIT", "Call initialize first")
        engine.subscribeChanges(object : ChangeListener {
            override fun onChange(notification: FfiChangeNotification) {
                try {
                    val params = Arguments.createMap().apply {
                        putString("table", notification.table)
                        putString("kind", notification.kind.name)
                        putString("primaryKey", notification.primaryKey)
                        notification.changedColumns?.let { cols ->
                            val arr = Arguments.createArray()
                            cols.forEach { arr.pushString(it) }
                            putArray("changedColumns", arr)
                        }
                    }
                    reactApplicationContext
                        .getJSModule(DeviceEventManagerModule.RCTDeviceEventEmitter::class.java)
                        .emit("WaveSyncChange", params)
                } catch (_: Exception) {
                    // Context may be destroyed; silently ignore
                }
            }
        })
        promise.resolve(true)
    }

    @ReactMethod
    fun subscribeNetworkEvents(promise: Promise) {
        val engine = ffi ?: return promise.reject("NOT_INIT", "Call initialize first")
        engine.subscribeNetworkEvents(object : NetworkEventListener {
            override fun onEvent(event: FfiNetworkEvent) {
                try {
                    val params = Arguments.createMap().apply {
                        when (event) {
                            is FfiNetworkEvent.PeerConnected -> {
                                putString("type", "PeerConnected")
                                putMap("peer", peerInfoToMap(event.peer))
                            }
                            is FfiNetworkEvent.PeerDisconnected -> {
                                putString("type", "PeerDisconnected")
                                putString("peerId", event.peerId)
                            }
                            is FfiNetworkEvent.PeerRejected -> {
                                putString("type", "PeerRejected")
                                putString("peerId", event.peerId)
                            }
                            is FfiNetworkEvent.PeerVerified -> {
                                putString("type", "PeerVerified")
                                putString("peerId", event.peerId)
                            }
                            is FfiNetworkEvent.PeerIdentityReceived -> {
                                putString("type", "PeerIdentityReceived")
                                putString("peerId", event.peerId)
                                putString("appId", event.appId)
                            }
                            is FfiNetworkEvent.RelayStatusChanged -> {
                                putString("type", "RelayStatusChanged")
                                putString("status", event.status.name)
                            }
                            is FfiNetworkEvent.NatStatusChanged -> {
                                putString("type", "NatStatusChanged")
                                putString("status", event.status.name)
                            }
                            is FfiNetworkEvent.RendezvousStatusChanged -> {
                                putString("type", "RendezvousStatusChanged")
                                putBoolean("registered", event.registered)
                            }
                            is FfiNetworkEvent.PeerSynced -> {
                                putString("type", "PeerSynced")
                                putString("peerId", event.peerId)
                                putDouble("dbVersion", event.dbVersion.toLong().toDouble())
                            }
                            is FfiNetworkEvent.EngineStarted -> {
                                putString("type", "EngineStarted")
                            }
                            is FfiNetworkEvent.EngineFailed -> {
                                putString("type", "EngineFailed")
                                putString("reason", event.reason)
                            }
                        }
                    }
                    reactApplicationContext
                        .getJSModule(DeviceEventManagerModule.RCTDeviceEventEmitter::class.java)
                        .emit("WaveSyncNetworkEvent", params)
                } catch (_: Exception) {
                    // Context may be destroyed; silently ignore
                }
            }
        })
        promise.resolve(true)
    }

    // -----------------------------------------------------------------------
    // Lifecycle & sync control
    // -----------------------------------------------------------------------

    @ReactMethod
    fun resume(promise: Promise) {
        val engine = ffi ?: return promise.reject("NOT_INIT", "Call initialize first")
        engine.resume()
        promise.resolve(true)
    }

    @ReactMethod
    fun networkTransition(promise: Promise) {
        val engine = ffi ?: return promise.reject("NOT_INIT", "Call initialize first")
        engine.networkTransition()
        promise.resolve(true)
    }

    @ReactMethod
    fun requestFullSync(promise: Promise) {
        val engine = ffi ?: return promise.reject("NOT_INIT", "Call initialize first")
        engine.requestFullSync()
        promise.resolve(true)
    }

    @ReactMethod
    fun isEngineAlive(promise: Promise) {
        val engine = ffi ?: return promise.reject("NOT_INIT", "Call initialize first")
        promise.resolve(engine.isEngineAlive())
    }

    @ReactMethod
    fun registerPushToken(platform: String, token: String, promise: Promise) {
        val engine = ffi ?: return promise.reject("NOT_INIT", "Call initialize first")
        engine.registerPushToken(platform, token)
        promise.resolve(true)
    }

    // -----------------------------------------------------------------------
    // Peer identity
    // -----------------------------------------------------------------------

    @ReactMethod
    fun setPeerIdentity(appId: String, promise: Promise) {
        val engine = ffi ?: return promise.reject("NOT_INIT", "Call initialize first")
        engine.setPeerIdentity(appId)
        promise.resolve(true)
    }

    @ReactMethod
    fun clearPeerIdentity(promise: Promise) {
        val engine = ffi ?: return promise.reject("NOT_INIT", "Call initialize first")
        engine.clearPeerIdentity()
        promise.resolve(true)
    }

    // -----------------------------------------------------------------------
    // Database metadata
    // -----------------------------------------------------------------------

    @ReactMethod
    fun nodeId(promise: Promise) {
        val engine = ffi ?: return promise.reject("NOT_INIT", "Call initialize first")
        promise.resolve(engine.nodeId())
    }

    @ReactMethod
    fun siteId(promise: Promise) {
        val engine = ffi ?: return promise.reject("NOT_INIT", "Call initialize first")
        promise.resolve(engine.siteId())
    }

    @ReactMethod
    fun readSyncConfig(promise: Promise) {
        val engine = ffi ?: return promise.reject("NOT_INIT", "Call initialize first")
        try {
            promise.resolve(engine.readSyncConfig())
        } catch (e: WaveSyncException) {
            promise.reject("WAVESYNC_ERR", e.message, e)
        }
    }

    @ReactMethod
    fun testBackgroundSync(timeoutSecs: Double, promise: Promise) {
        val engine = ffi ?: return promise.reject("NOT_INIT", "Call initialize first")
        val dbUrl = engine.readSyncConfig().let { json ->
            Regex(""""database_url"\s*:\s*"([^"]+)"""").find(json)?.groupValues?.get(1)
        } ?: return promise.reject("WAVESYNC_ERR", "Cannot extract database_url from config")

        scope.launch {
            try {
                // Shutdown the current engine first — background_sync creates its own
                engine.shutdown()
                ffi = null

                val result = uniffi.wavesyncdb_ffi.backgroundSync(
                    dbUrl,
                    timeoutSecs.toInt().toUInt(),
                    null
                )
                val map = Arguments.createMap().apply {
                    when (result) {
                        is FfiBackgroundSyncResult.Synced -> {
                            putString("status", "synced")
                            putInt("peersSynced", result.peersSynced.toInt())
                        }
                        is FfiBackgroundSyncResult.NoPeers -> {
                            putString("status", "no_peers")
                            putInt("peersSynced", 0)
                        }
                        is FfiBackgroundSyncResult.TimedOut -> {
                            putString("status", "timed_out")
                            putInt("peersSynced", result.peersSynced.toInt())
                        }
                    }
                }
                promise.resolve(map)
            } catch (e: Exception) {
                promise.reject("WAVESYNC_ERR", e.message, e)
            }
        }
    }

    // -----------------------------------------------------------------------
    // Shutdown
    // -----------------------------------------------------------------------

    @ReactMethod
    fun shutdown(promise: Promise) {
        val engine = ffi ?: return promise.resolve(true)
        scope.launch {
            try {
                engine.shutdown()
                ffi = null
                promise.resolve(true)
            } catch (e: Exception) {
                promise.reject("WAVESYNC_ERR", e.message, e)
            }
        }
    }

    // Required stubs for NativeEventEmitter
    @ReactMethod
    fun addListener(eventName: String) {}

    @ReactMethod
    fun removeListeners(count: Double) {}

    @Deprecated("Deprecated in ReactContextBaseJavaModule")
    override fun onCatalystInstanceDestroy() {
        val engine = ffi ?: return
        ffi = null
        // Await engine shutdown so the SQLite connection is released before
        // the next initialize() call.  Without this, hot-reload or app restart
        // can hit "database is locked" because the old engine is still running.
        runBlocking {
            withTimeoutOrNull(5000) {
                engine.shutdown()
            }
        }
        scope.cancel()
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private fun peerInfoToMap(peer: FfiPeerInfo): WritableMap {
        return Arguments.createMap().apply {
            putString("peerId", peer.peerId)
            putString("address", peer.address)
            peer.dbVersion?.let {
                putDouble("dbVersion", it.toLong().toDouble())
            }
            putBoolean("isBootstrap", peer.isBootstrap)
            putBoolean("isGroupMember", peer.isGroupMember)
            peer.appId?.let { putString("appId", it) }
        }
    }
}
