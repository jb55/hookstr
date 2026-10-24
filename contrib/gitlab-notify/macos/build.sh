#!/bin/sh
# Build and sign GitLabNotify.app, gitlab-notify's macOS notifier, into
# build/ next to this script. gitlab-notify.py picks it up from there.
#
#   contrib/gitlab-notify/macos/build.sh
#
# CODESIGN_IDENTITY picks the signing identity (`security find-identity -v
# -p codesigning` lists them); "-" signs ad hoc. SWIFT_DEVELOPER_DIR picks
# the toolchain, the Command Line Tools by default. It's not DEVELOPER_DIR,
# which a nix shell points at a nix SDK with no swiftc.
set -eu

here=$(cd "$(dirname "$0")" && pwd)
identity=${CODESIGN_IDENTITY-"Apple Development: Bill Casarin (EYLQ67VC2R)"}
dev=${SWIFT_DEVELOPER_DIR:-/Library/Developer/CommandLineTools}

app=$here/build/GitLabNotify.app
tmp=$here/build/tmp
rm -rf "$app" "$tmp"
mkdir -p "$app/Contents/MacOS" "$app/Contents/Resources" "$tmp/AppIcon.iconset"

env -u SDKROOT DEVELOPER_DIR="$dev" "$dev/usr/bin/swiftc" -O -swift-version 5 \
    -sdk "$dev/SDKs/MacOSX.sdk" \
    -o "$app/Contents/MacOS/GitLabNotify" "$here/main.swift"
cp "$here/Info.plist" "$app/Contents/Info.plist"

# icons/app.png is 1024px, drawn by make-icons.py.
png=$here/../icons/app.png
for s in 16 32 128 256 512; do
    /usr/bin/sips -z $s $s "$png" --out "$tmp/AppIcon.iconset/icon_${s}x${s}.png" >/dev/null
    d=$((s * 2))
    /usr/bin/sips -z $d $d "$png" --out "$tmp/AppIcon.iconset/icon_${s}x${s}@2x.png" >/dev/null
done
/usr/bin/iconutil -c icns -o "$app/Contents/Resources/AppIcon.icns" "$tmp/AppIcon.iconset"
rm -rf "$tmp"

/usr/bin/codesign --force --options runtime --timestamp=none --sign "$identity" "$app"
/usr/bin/codesign --verify --strict "$app"
# Until LaunchServices knows the bundle, posting from the bare executable
# fails with "Notifications are not allowed for this application".
/System/Library/Frameworks/CoreServices.framework/Frameworks/LaunchServices.framework/Support/lsregister -f "$app"
echo "built $app"
