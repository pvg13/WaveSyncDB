#import <Foundation/Foundation.h>

NS_ASSUME_NONNULL_BEGIN

/// Reference-counted wrapper around a `UIBackgroundFetchResult` completion
/// block, used to ferry the block across the ObjC → Swift `@_cdecl` bridge.
///
/// An ObjC block pointer cannot be safely cast to a Swift `UnsafeRawPointer`
/// and bridged back, because block memory layout is a private implementation
/// detail. Wrapping the block in an `NSObject` means Swift can hold the
/// wrapper as a strong reference and call `-invokeWithResult:` when the
/// async sync operation finishes.
///
/// The wrapper nil-s out its held block after the first invocation to guard
/// against double-invocation (which UIKit considers a hard error).
@interface WaveSyncCompletionWrapper : NSObject

- (instancetype)initWithBlock:(void (^)(NSInteger))block NS_DESIGNATED_INITIALIZER;
- (instancetype)init NS_UNAVAILABLE;
+ (instancetype)new NS_UNAVAILABLE;

- (void)invokeWithResult:(NSInteger)result;

@end

NS_ASSUME_NONNULL_END
