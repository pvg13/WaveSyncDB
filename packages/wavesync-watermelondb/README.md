# @wavesync/watermelondb

WatermelonDB adapter for WaveSyncDB. Define your schema, use WatermelonDB
normally, get P2P sync for free.

## Installation

```bash
npm install @wavesync/react-native @wavesync/watermelondb @nozbe/watermelondb
```

Add decorator support to `babel.config.js`:
```javascript
plugins: [['@babel/plugin-proposal-decorators', { legacy: true }]]
```

See [@wavesync/react-native](https://www.npmjs.com/package/@wavesync/react-native)
for required Gradle configuration (`abiFilters` + `useLegacyPackaging`).

## Usage

```typescript
import { Database } from '@nozbe/watermelondb';
import { appSchema, tableSchema } from '@nozbe/watermelondb';
import { Model } from '@nozbe/watermelondb';
import { field } from '@nozbe/watermelondb/decorators';
import { WaveSyncAdapter } from '@wavesync/watermelondb';

// 1. Define schema
const schema = appSchema({
  version: 1,
  tables: [
    tableSchema({
      name: 'tasks',
      columns: [
        { name: 'title', type: 'string' },
        { name: 'done',  type: 'boolean' },
      ],
    }),
  ],
});

// 2. Define model
class Task extends Model {
  static table = 'tasks';
  @field('title') title!: string;
  @field('done')  done!: boolean;
}

// 3. Create database with P2P sync
const adapter = new WaveSyncAdapter({
  schema,
  topic:      'my-app-group',
  passphrase: 'shared-secret',
});

const database = new Database({
  adapter: adapter as any,
  modelClasses: [Task],
});

// Required: enables remote change propagation to WatermelonDB's reactive system
adapter.setDatabase(database);

// 4. Use WatermelonDB normally — sync is automatic
await database.write(async () => {
  await database.get<Task>('tasks').create(task => {
    task.title = 'Buy groceries';
    task.done = false;
  });
});
```

## Reactivity

Both local writes and remote peer changes trigger reactive re-renders:

```typescript
import { withObservables } from '@nozbe/with-observables';

const enhance = withObservables([], () => ({
  tasks: database.get<Task>('tasks').query().observe(),
}));

const TaskList = enhance(({ tasks }) => (
  <FlatList data={tasks} renderItem={({ item }) => <Text>{item.title}</Text>} />
));
```

## How it works

`WaveSyncAdapter` implements WatermelonDB's `DatabaseAdapter` interface.
All writes go through WaveSyncDB's CRDT engine and sync P2P via libp2p.
Remote changes are bridged back into WatermelonDB's notification system
(`Database._notify` / `Collection._applyChangesToCache`), triggering
`query().observe()` and `withObservables` re-renders automatically.

The developer writes standard WatermelonDB code. P2P sync is invisible.
