module.exports = {
  dependency: {
    platforms: {
      android: {
        sourceDir: './android',
        packageImportPath: 'import com.wavesync.WaveSyncPackage;',
        packageInstance: 'new WaveSyncPackage()',
      },
      ios: {},
    },
  },
};
