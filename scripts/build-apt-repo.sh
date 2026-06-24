#!/usr/bin/env bash
# Rebuild the apt repo index (Approach A: from the FULL pool) and GPG-sign it.
# The given dir must already contain pool/ with every .deb to publish (pull the
# existing pool first, then drop the new .debs in).
#
#   build-apt-repo.sh <repo-dir>
#
# Env: APT_GPG_KEY_ID (required); APT_CODENAME, APT_COMPONENT, APT_ARCHES,
#      APT_ORIGIN, APT_LABEL (optional).
set -euo pipefail

REPO_DIR="${1:?usage: build-apt-repo.sh <repo-dir>}"
CODENAME="${APT_CODENAME:-stable}"
COMPONENT="${APT_COMPONENT:-main}"
ARCHES="${APT_ARCHES:-amd64 arm64}"
ORIGIN="${APT_ORIGIN:-ComputeStacks}"
LABEL="${APT_LABEL:-cs-agent}"
GPG_KEY="${APT_GPG_KEY_ID:?APT_GPG_KEY_ID required}"

cd "$REPO_DIR"

if ! ls pool/**/*.deb >/dev/null 2>&1 && ! find pool -name '*.deb' | grep -q .; then
  echo "build-apt-repo: no .deb files under $REPO_DIR/pool" >&2
  exit 1
fi

# Per-arch package indexes. Run from repo root so each stanza's Filename is
# "pool/..." (relative to where the sources.list URL points).
for arch in $ARCHES; do
  d="dists/$CODENAME/$COMPONENT/binary-$arch"
  mkdir -p "$d"
  apt-ftparchive --arch "$arch" packages pool > "$d/Packages"
  gzip -kf "$d/Packages"
done

# Top-level Release (lists hashes+sizes of the Packages files apt-ftparchive finds below).
conf="$(mktemp)"
cat > "$conf" <<EOF
APT::FTPArchive::Release::Origin "$ORIGIN";
APT::FTPArchive::Release::Label "$LABEL";
APT::FTPArchive::Release::Suite "$CODENAME";
APT::FTPArchive::Release::Codename "$CODENAME";
APT::FTPArchive::Release::Architectures "$ARCHES";
APT::FTPArchive::Release::Components "$COMPONENT";
APT::FTPArchive::Release::Description "cs-agent apt repository";
EOF
apt-ftparchive -c "$conf" release "dists/$CODENAME" > "dists/$CODENAME/Release"
rm -f "$conf"

# Sign: detached (Release.gpg) + inline (InRelease, preferred by modern apt).
gpg --batch --yes --default-key "$GPG_KEY" -abs -o "dists/$CODENAME/Release.gpg" "dists/$CODENAME/Release"
gpg --batch --yes --default-key "$GPG_KEY" --clearsign -o "dists/$CODENAME/InRelease" "dists/$CODENAME/Release"

echo "build-apt-repo: index rebuilt + signed for codename=$CODENAME arches=[$ARCHES]"
# Follow-up: enable by-hash (APT::FTPArchive::DoByHash) for fully atomic updates.
