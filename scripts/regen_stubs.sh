#!/usr/bin/env bash
# Regenerate the meeting.v1 stubs that ship inside the package.
#
# Unlike every other stub in this repo, these are COMMITTED. The package is
# installed by people on their own hosts with a plain `pip install`, and a
# generate-at-build-time hook would demand protoc and grpcio-tools on THEIR
# machine whenever pip builds from the sdist. Shipping the stubs means the
# package carries the contract version it was built against, and installing it
# needs no toolchain at all.
#
# Run after changing proto/meeting/v1/meeting.proto. CI checks the result is
# current (see .github/workflows/validate.yml) so the two cannot drift.
#
#   scripts/regen_stubs.sh
#
# The generator version is pinned because it is stamped INTO the output (a
# "Protobuf Python Version" header and a ValidateProtobufRuntimeVersion call).
# An unpinned generator would make the CI freshness check fail the moment
# grpcio-tools releases, and would silently move the protobuf floor that
# pyproject.toml declares. Bumping it here means: regenerate, run the tests, and
# raise `protobuf>=` to match.
set -euo pipefail

GRPCIO_TOOLS_VERSION="${GRPCIO_TOOLS_VERSION:-1.83.0}"

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/.." && pwd)"
out="$repo/src/talk_capture/_pb"
proto="$repo/proto/meeting/v1/meeting.proto"

[ -f "$proto" ] || { echo "missing contract: $proto" >&2; exit 1; }

have="$(python -c 'import grpc_tools, importlib.metadata as m; print(m.version("grpcio-tools"))' 2>/dev/null || echo none)"
if [ "$have" != "$GRPCIO_TOOLS_VERSION" ]; then
    echo "grpcio-tools $GRPCIO_TOOLS_VERSION required, found $have" >&2
    echo "  pip install 'grpcio-tools==$GRPCIO_TOOLS_VERSION'" >&2
    exit 1
fi

python -m grpc_tools.protoc \
    -I "$(dirname "$proto")" \
    --python_out="$out" \
    --grpc_python_out="$out" \
    "$proto"

# protoc emits a bare `import meeting_pb2`, which resolves only when the stub
# directory itself is on sys.path. Inside a package it is not, so rewrite it to
# a package-relative import (same fix the orchestrator's Dockerfile applies to
# the telemost stubs).
sed -i.bak 's/^import meeting_pb2 as /from talk_capture._pb import meeting_pb2 as /' \
    "$out/meeting_pb2_grpc.py"
rm -f "$out/meeting_pb2_grpc.py.bak"

echo "regenerated: $out"
