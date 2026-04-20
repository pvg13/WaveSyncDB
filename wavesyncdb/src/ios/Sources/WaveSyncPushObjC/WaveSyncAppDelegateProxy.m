// WaveSyncAppDelegateProxy — automatic APNs AppDelegate integration.
//
// The `+load` method runs at image load, before `UIApplicationMain`.
// Since the `UIApplicationDelegate` instance is not yet available at that
// point, `+load` installs an observer for `UIApplicationDidFinishLaunchingNotification`.
// When that fires, the observer:
//
//   1. Captures the current application delegate class.
//   2. Installs three APNs-related selectors on it via `class_addMethod`.
//      If a selector is already implemented by the delegate (i.e. the host
//      app has its own custom AppDelegate), we log an error and skip that
//      selector. Chaining with pre-existing implementations is intentionally
//      deferred until v1.1 to keep the initial surface small.
//   3. Forwards the `UIApplicationLaunchOptionsRemoteNotificationKey` payload
//      (if the app was launched *by* a silent push) into the receive path.
//   4. Calls `-[UIApplication registerForRemoteNotifications]` so the OS
//      produces a device token.
//
// The installed method implementations are plain C functions that forward
// to Swift `@_cdecl` entry points in `WaveSyncPushBridge.swift`.

#import "WaveSyncAppDelegateProxy.h"
#import "WaveSyncCompletionWrapper.h"

#import <UIKit/UIKit.h>
#import <objc/runtime.h>
#import <objc/message.h>

// Swift @_cdecl entry points. Defined in Sources/WaveSyncPush/WaveSyncPushBridge.swift.
//
// `device_token` is an NSData *, passed as an opaque pointer and bridged
// back in Swift via `Unmanaged.fromOpaque(_).takeUnretainedValue()`.
// `error` is an NSError *, handled the same way.
// `user_info` is an NSDictionary *, handled the same way.
// `completion_wrapper` is a WaveSyncCompletionWrapper *, passed with a
// retained handoff — Swift takes ownership via `takeRetainedValue()`.
extern void wavesync_push_bridge_did_register(const void *device_token);
extern void wavesync_push_bridge_did_fail(const void *error);
extern void wavesync_push_bridge_did_receive(const void *user_info, const void *completion_wrapper);

#pragma mark - Swizzled method implementations

static void waveSync_application_didRegisterForRemoteNotificationsWithDeviceToken(
    id self,
    SEL _cmd,
    UIApplication *application,
    NSData *deviceToken)
{
    wavesync_push_bridge_did_register((__bridge const void *)deviceToken);
}

static void waveSync_application_didFailToRegisterForRemoteNotificationsWithError(
    id self,
    SEL _cmd,
    UIApplication *application,
    NSError *error)
{
    wavesync_push_bridge_did_fail((__bridge const void *)error);
}

static void waveSync_application_didReceiveRemoteNotification_fetchCompletionHandler(
    id self,
    SEL _cmd,
    UIApplication *application,
    NSDictionary *userInfo,
    void (^completionHandler)(UIBackgroundFetchResult))
{
    WaveSyncCompletionWrapper *wrapper = [[WaveSyncCompletionWrapper alloc]
        initWithBlock:^(NSInteger r) {
            completionHandler((UIBackgroundFetchResult)r);
        }];
    // `__bridge_retained` transfers ownership to the pointer; Swift balances
    // it with `Unmanaged.takeRetainedValue()`.
    wavesync_push_bridge_did_receive(
        (__bridge const void *)userInfo,
        (__bridge_retained const void *)wrapper);
}

#pragma mark - Installation

// Try to add `sel` to `cls` pointing at `imp`. If the class already implements
// the selector, leave the existing implementation in place and log a warning —
// we do not swap (see v1 scope note at the top of the file).
static void install_method(Class cls, SEL sel, IMP imp, const char *encoding) {
    if (class_getInstanceMethod(cls, sel) != NULL) {
        NSLog(@"[WaveSync] App delegate already implements %@ — skipping injection. "
              @"Custom APNs delegate methods are not supported in v1; remove them "
              @"or file an issue for v1.1 chain-through support.",
              NSStringFromSelector(sel));
        return;
    }
    if (!class_addMethod(cls, sel, imp, encoding)) {
        NSLog(@"[WaveSync] class_addMethod failed for %@", NSStringFromSelector(sel));
    }
}

// Dispatch a remote notification that was delivered via `launchOptions`
// (cold-start case). iOS does *not* call `application:didReceiveRemoteNotification:`
// automatically when the app is launched from a terminated state by a silent
// push — the payload arrives only in the launch options dictionary.
static void dispatch_cold_start_notification(UIApplication *application, NSDictionary *launchOptions) {
    if (launchOptions == nil) { return; }
    id payload = launchOptions[UIApplicationLaunchOptionsRemoteNotificationKey];
    if (![payload isKindOfClass:[NSDictionary class]]) { return; }

    // Synthesise a completion handler. We don't have one from UIKit in the
    // cold-start path; `beginBackgroundTaskWithExpirationHandler:` keeps the
    // process alive until our sync finishes or iOS runs out of background time.
    __block UIBackgroundTaskIdentifier bgTask = UIBackgroundTaskInvalid;
    bgTask = [application beginBackgroundTaskWithName:@"WaveSyncColdStartPush"
                                    expirationHandler:^{
        [application endBackgroundTask:bgTask];
    }];

    WaveSyncCompletionWrapper *wrapper = [[WaveSyncCompletionWrapper alloc]
        initWithBlock:^(NSInteger result) {
            if (bgTask != UIBackgroundTaskInvalid) {
                [application endBackgroundTask:bgTask];
                bgTask = UIBackgroundTaskInvalid;
            }
        }];

    wavesync_push_bridge_did_receive(
        (__bridge const void *)payload,
        (__bridge_retained const void *)wrapper);
}

static void install_delegate_methods(NSNotification *note) {
    UIApplication *application = note.object;
    id <UIApplicationDelegate> delegate = application.delegate;
    if (delegate == nil) {
        NSLog(@"[WaveSync] No UIApplicationDelegate found at DidFinishLaunching — "
              @"push notifications will not be received");
        return;
    }

    Class cls = object_getClass(delegate);

    install_method(
        cls,
        @selector(application:didRegisterForRemoteNotificationsWithDeviceToken:),
        (IMP)waveSync_application_didRegisterForRemoteNotificationsWithDeviceToken,
        "v@:@@");

    install_method(
        cls,
        @selector(application:didFailToRegisterForRemoteNotificationsWithError:),
        (IMP)waveSync_application_didFailToRegisterForRemoteNotificationsWithError,
        "v@:@@");

    install_method(
        cls,
        @selector(application:didReceiveRemoteNotification:fetchCompletionHandler:),
        (IMP)waveSync_application_didReceiveRemoteNotification_fetchCompletionHandler,
        "v@:@@@@?");

    // Cold-start push payload, if any.
    dispatch_cold_start_notification(application, note.userInfo);

    // Ask iOS for a device token. The swizzled `didRegister…` handler will fire
    // asynchronously once APNs responds.
    [application registerForRemoteNotifications];

    NSLog(@"[WaveSync] APNs delegate methods installed on %@", NSStringFromClass(cls));
}

@implementation WaveSyncAppDelegateProxy

+ (void)load {
    // `+load` runs before `main()`. Defer the actual installation until after
    // `UIApplicationMain` has set up the delegate.
    [[NSNotificationCenter defaultCenter]
        addObserverForName:UIApplicationDidFinishLaunchingNotification
                    object:nil
                     queue:[NSOperationQueue mainQueue]
                usingBlock:^(NSNotification *note) {
        install_delegate_methods(note);
    }];
    NSLog(@"[WaveSync] AppDelegate proxy scheduled for DidFinishLaunching");
}

@end
