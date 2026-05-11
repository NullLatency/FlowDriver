#!/bin/bash
set -e

VERSION="v1.0.0"
RELEASE_DIR="release"

# Clean previous releases
rm -rf "$RELEASE_DIR"
mkdir -p "$RELEASE_DIR"

platforms=(
    "linux/amd64"
    "windows/amd64"
    "darwin/amd64"
    "darwin/arm64"
)

echo "Building binaries for version $VERSION..."

for platform in "${platforms[@]}"; do
    OS=${platform%/*}
    ARCH=${platform#*/}
    
    SUFFIX=""
    if [ "$OS" == "windows" ]; then
        SUFFIX=".exe"
    fi
    
    FOLDER_NAME="flow-driver-${VERSION}-${OS}-${ARCH}"
    OUTPUT_PATH="$RELEASE_DIR/$FOLDER_NAME"
    mkdir -p "$OUTPUT_PATH"

    echo "Building $OS/$ARCH..."
    
    # Build Client
    CGO_ENABLED=0 GOOS=$OS GOARCH=$ARCH /opt/homebrew/bin/go build -o "$OUTPUT_PATH/client$SUFFIX" ./cmd/client
    
    # Build Server
    CGO_ENABLED=0 GOOS=$OS GOARCH=$ARCH /opt/homebrew/bin/go build -o "$OUTPUT_PATH/server$SUFFIX" ./cmd/server
    
    # Copy Example Configs and README
    cp client_config.json.example "$OUTPUT_PATH/"
    cp server_config.json.example "$OUTPUT_PATH/"
    cp scripts/flowdriver-server.service "$OUTPUT_PATH/"
    cp README.md "$OUTPUT_PATH/"
    
    # Zip it up
    (cd "$RELEASE_DIR" && zip -r "${FOLDER_NAME}.zip" "$FOLDER_NAME")
    
    # Cleanup folder (keep only zip)
    rm -rf "$OUTPUT_PATH"
done

echo "Done! Release packages created in $RELEASE_DIR/:"
ls -lh "$RELEASE_DIR"
