/**
 * @format
 */

import { AppRegistry } from 'react-native';
import { registerWaveSyncBackgroundHandler } from '@wavesync/react-native';
import App from './App';
import { name as appName } from './app.json';

// Must be called before AppRegistry.registerComponent
registerWaveSyncBackgroundHandler();

AppRegistry.registerComponent(appName, () => App);
