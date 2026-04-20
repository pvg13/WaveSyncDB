#import "WaveSyncCompletionWrapper.h"

@implementation WaveSyncCompletionWrapper {
    void (^_block)(NSInteger);
}

- (instancetype)initWithBlock:(void (^)(NSInteger))block {
    self = [super init];
    if (self) {
        _block = [block copy];
    }
    return self;
}

- (void)invokeWithResult:(NSInteger)result {
    void (^block)(NSInteger) = _block;
    _block = nil;
    if (block) {
        block(result);
    }
}

@end
