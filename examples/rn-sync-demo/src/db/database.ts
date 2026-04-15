import {Database} from '@nozbe/watermelondb';
import {WaveSyncAdapter} from '@wavesync/watermelondb';
import schema from './schema';
import {Task} from './Task';

export const adapter = new WaveSyncAdapter({
  schema,
  topic: 'wavesync-demo',
  passphrase: 'demo-secret-42',
  relayAddr:
    "/dns4/relay.roommatesapp.es/tcp/4001/p2p/12D3KooWSH8G4zDwzK2srm8u6DWxvFiY6emuJkKSKswvGiG2i8qh",
  rendezvousAddr:
    "/dns4/relay.roommatesapp.es/tcp/4001/p2p/12D3KooWSH8G4zDwzK2srm8u6DWxvFiY6emuJkKSKswvGiG2i8qh",
});

export const database = new Database({
  adapter: adapter as any,
  modelClasses: [Task],
});

adapter.setDatabase(database);
