const {getDefaultConfig, mergeConfig} = require('@react-native/metro-config');
const path = require('path');

const monorepoRoot = path.resolve(__dirname, '../..');

/**
 * Metro configuration
 * https://reactnative.dev/docs/metro
 *
 * @type {import('@react-native/metro-config').MetroConfig}
 */
const config = {
  watchFolders: [
    path.resolve(monorepoRoot, 'packages/wavesync-rn'),
    path.resolve(monorepoRoot, 'packages/wavesync-watermelondb'),
  ],
  resolver: {
    // When Metro resolves imports from watched package folders, it needs to
    // find dependencies (react-native, @babel/runtime, etc.) in the example
    // app's node_modules — the packages themselves don't have node_modules.
    nodeModulesPaths: [path.resolve(__dirname, 'node_modules')],
  },
};

module.exports = mergeConfig(getDefaultConfig(__dirname), config);
