import {Platform} from 'react-native';
import {WaveSync} from './WaveSync';

/**
 * Initialize FCM for WaveSyncDB push wakeup.
 * Call once after WaveSync.initialize() has completed.
 *
 * Automatically:
 * - Requests notification permission (Android 13+)
 * - Gets the FCM token
 * - Registers the token with WaveSyncDB (forwarded to relay)
 * - Handles token refresh
 * - Sets up foreground push handler to trigger sync
 *
 * Requires @react-native-firebase/messaging as a peer dependency.
 * If not installed, logs a warning and returns without error.
 */
export async function initWaveSyncFCM(): Promise<void> {
  let messaging: any;
  try {
    messaging = require('@react-native-firebase/messaging').default;
  } catch {
    console.warn(
      '@wavesync/react-native: @react-native-firebase/messaging not found. ' +
        'Install it to enable push wakeup for background sync.',
    );
    return;
  }

  // Request permission (required on Android 13+ and iOS)
  try {
    const authStatus = await messaging().requestPermission();
    const enabled =
      authStatus === messaging.AuthorizationStatus.AUTHORIZED ||
      authStatus === messaging.AuthorizationStatus.PROVISIONAL;

    if (!enabled) {
      console.warn('@wavesync/react-native: FCM permission denied');
      return;
    }
  } catch (e: any) {
    console.warn('@wavesync/react-native: FCM permission request failed:', e?.message);
    return;
  }

  // Get token and register with WaveSyncDB → relay
  try {
    const token = await messaging().getToken();
    const platform = Platform.OS === 'ios' ? 'apns' : 'fcm';
    await WaveSync.registerPushToken(token);
    console.log(`@wavesync/react-native: ${platform} token registered`);
  } catch (e: any) {
    console.warn('@wavesync/react-native: FCM token registration failed:', e?.message);
    return;
  }

  // Handle token refresh
  messaging().onTokenRefresh(async (newToken: string) => {
    try {
      await WaveSync.registerPushToken(newToken);
      console.log('@wavesync/react-native: FCM token refreshed and registered');
    } catch (e: any) {
      console.warn('@wavesync/react-native: token refresh registration failed:', e?.message);
    }
  });

  // Handle silent push in foreground — trigger sync
  messaging().onMessage(async (remoteMessage: any) => {
    if (remoteMessage.data?.type === 'sync_available') {
      console.log('@wavesync/react-native: sync_available push received (foreground)');
      try {
        await WaveSync.resume();
      } catch {
        // Engine may not be alive; ignore
      }
    }
  });
}

/**
 * Register the background message handler for WaveSyncDB push wakeup.
 * Must be called in index.js BEFORE AppRegistry.registerComponent().
 *
 * When the app is in the background, this handler triggers a catch-up sync
 * via resume(). When the app is killed, the native WaveSyncService handles
 * the push directly via background_sync() — no JS is involved.
 */
export function registerWaveSyncBackgroundHandler(): void {
  let messaging: any;
  try {
    messaging = require('@react-native-firebase/messaging').default;
  } catch {
    return; // Firebase not installed — skip silently
  }

  messaging().setBackgroundMessageHandler(async (remoteMessage: any) => {
    if (remoteMessage.data?.type === 'sync_available') {
      console.log('@wavesync/react-native: sync_available push received (background)');
      // When the RN bridge is alive (app backgrounded but not killed),
      // trigger a catch-up sync. When the app is killed, onMessageReceived
      // in WaveSyncService.kt handles it natively via background_sync().
      try {
        await WaveSync.resume();
      } catch {
        // Engine not alive in headless mode — WaveSyncService handles it
      }
    }
  });
}
