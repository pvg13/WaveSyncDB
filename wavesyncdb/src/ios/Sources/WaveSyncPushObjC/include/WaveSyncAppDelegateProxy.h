#import <Foundation/Foundation.h>

NS_ASSUME_NONNULL_BEGIN

/// Sentinel class whose `+load` method installs the APNs selectors on the
/// application delegate at image-load time. No public API — exposing the
/// class header only so that Swift can take a static reference to it
/// (see `WaveSyncTokenStore.keepProxyAlive`), which prevents the linker
/// from dead-stripping the class from the static archive.
@interface WaveSyncAppDelegateProxy : NSObject
@end

NS_ASSUME_NONNULL_END
