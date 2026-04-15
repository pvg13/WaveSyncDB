require 'json'

package = JSON.parse(File.read(File.join(__dir__, 'package.json')))

Pod::Spec.new do |s|
  s.name         = "wavesync-react-native"
  s.version      = package['version']
  s.summary      = package['description']
  s.homepage     = "https://github.com/anthropics/wavesyncdb"
  s.license      = "MIT"
  s.author       = "WaveSyncDB"
  s.source       = { :path => "." }
  s.platforms    = { :ios => "14.0" }

  s.source_files = "ios/*.{swift,m,h}", "ios/Generated/*.swift"
  s.vendored_frameworks = "ios/Frameworks/wavesyncdb_ffi.xcframework"

  s.preserve_paths = "ios/Generated/wavesyncdb_ffiFFI.modulemap",
                     "ios/Generated/wavesyncdb_ffiFFI.h"

  s.pod_target_xcconfig = {
    "SWIFT_INCLUDE_PATHS" => "$(PODS_TARGET_SRCROOT)/ios/Generated",
    "HEADER_SEARCH_PATHS" => "$(PODS_TARGET_SRCROOT)/ios/Generated",
    # Link system frameworks required by libp2p/tokio
    "OTHER_LDFLAGS" => "-lc++ -lresolv",
  }

  s.dependency "React-Core"
end
