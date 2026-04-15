# @wavesync/react-native

React Native bindings for WaveSyncDB — transparent P2P SQLite sync for mobile apps.

## Installation

```bash
npm install @wavesync/react-native
```

React Native autolinking handles native module registration automatically.

### Required app-level Gradle config

Add these two blocks to your app's `android/app/build.gradle` inside `android {}`:

```groovy
android {
    defaultConfig {
        ndk {
            // WaveSyncDB ships 64-bit only
            abiFilters "arm64-v8a", "x86_64"
        }
    }
    packagingOptions {
        jniLibs {
            // Required for JNA to load native libraries from APK
            useLegacyPackaging = true
        }
    }
}
```

## Usage

```typescript
import { WaveSync } from '@wavesync/react-native';

// Initialize with a topic (peers with the same topic discover each other)
await WaveSync.initialize('my-app-topic', {
  passphrase: 'shared-secret',
});

// Register a table for sync
await WaveSync.registerSyncedTable('tasks', 'id', ['title', 'done'], createSql);
await WaveSync.registryReady();
await WaveSync.subscribeChanges();

// All writes sync P2P automatically
await WaveSync.execute('INSERT INTO tasks (id, title, done) VALUES (...)');

// Query
const tasks = await WaveSync.query<{ id: string; title: string }>('SELECT * FROM tasks');

// Listen for changes from remote peers
const subscription = WaveSync.onChangeEvent((change) => {
  console.log('Remote change:', change.table, change.kind, change.primaryKey);
});

// Network status
const status = await WaveSync.networkStatus();
console.log(`Connected to ${status.peerCount} peers`);
```

## With WatermelonDB

For a zero-config ORM experience, see
[@wavesync/watermelondb](https://www.npmjs.com/package/@wavesync/watermelondb).

## API

### `WaveSync.initialize(topic, options?)`
Initialize the sync engine with a topic and optional passphrase/relay config.

### `WaveSync.execute(sql)`
Execute a write SQL statement. Returns rows affected.

### `WaveSync.query<T>(sql)`
Execute a read SQL query. Returns parsed JSON rows.

### `WaveSync.networkStatus()`
Returns current peer count, relay status, NAT status, and connected peers.

### `WaveSync.onChangeEvent(callback)`
Subscribe to remote change notifications. Returns a subscription with `.remove()`.

### `WaveSync.shutdown()`
Shut down the sync engine cleanly.
