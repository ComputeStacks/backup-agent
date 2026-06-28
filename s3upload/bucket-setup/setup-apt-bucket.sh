#!/usr/bin/env bash
# Provision the PUBLIC apt repository bucket on UpCloud Managed Object Storage
# (Dell ECS backend). Idempotent and additive — safe to re-run.
#
# This is the OSS / public-read apt repo: ONE bucket holds the whole repo
# (dists/ + pool/) for every ComputeStacks OSS package, served anonymously over
# HTTPS so nodes `apt-get` with NO credentials. It is the read-OPEN counterpart
# to setup-export-bucket.sh (private, presigned, 1-day expiry); see that script's
# README for the shared ECS gotchas (IAM on :4443, `when_required` checksums).
#
# Given a bucket (and optionally a publisher group + user) it will:
#   1. create the BUCKET if absent;
#   2. apply an anonymous public-read **GetObject** bucket policy — GetObject
#      ONLY (no ListBucket), so the bucket can't be enumerated; apt fetches known
#      paths from the signed Release index. (Verified: ECS honors a Principal:"*"
#      policy and serves anonymous HTTPS GET, with a real 404 on a missing key.);
#   3. if GROUP is set: create it if absent and ensure a group-managed policy
#      grants the CI publisher PutObject/GetObject/DeleteObject on <bucket>/* +
#      ListBucket on <bucket>. ADDITIVE — an existing policy gains the bucket via
#      a new policy version (other buckets keep access);
#   4. if USER is set: create it if absent and add it to GROUP. CREATE_KEY=1 mints
#      an access key and writes it to ./<user>.creds (chmod 600) — that pair is
#      the GitHub `release` Environment's APT_S3_ACCESS_KEY / APT_S3_SECRET_KEY.
#      The SECRET is written to the file only, never printed to the terminal.
#   5. print the GitHub Environment vars + the node sources.list line.
#
# NO object-expiry lifecycle — pool/ is append-only (every version stays
# installable for rollback). ABORT_MPU_DAYS>0 applies an abort-incomplete-
# multipart rule (hygiene only, no object expiry); off by default.
#
# Run with an account/owner key (the cs-repo profile). Usage:
#   PROFILE=cs-repo ENDPOINT=https://repo.computestacks.com \
#   BUCKET=public GROUP=apt-publishers IAM_USER=apt-publisher CREATE_KEY=1 \
#   ./setup-apt-bucket.sh
#
#   # bucket + public policy only (no IAM):
#   PROFILE=cs-repo ENDPOINT=https://repo.computestacks.com BUCKET=public ./setup-apt-bucket.sh
set -euo pipefail

: "${ENDPOINT:?set ENDPOINT, e.g. https://repo.computestacks.com}"
: "${BUCKET:=public}"
PROFILE="${PROFILE:-}"
GROUP="${GROUP:-}"
IAM_USER="${IAM_USER:-}"
if [ -n "$IAM_USER" ] && [ -z "$GROUP" ]; then echo "IAM_USER requires GROUP (the group to add it to)"; exit 2; fi
POLICY="${POLICY:-apt-publish}"
ABORT_MPU_DAYS="${ABORT_MPU_DAYS:-0}"   # 0 = no lifecycle at all
SUITE="${SUITE:-stable}"
COMPONENT="${COMPONENT:-main}"

# aws wrapper: inject --profile when set, so every call uses the same account.
aws_() { if [ -n "$PROFILE" ]; then aws --profile "$PROFILE" "$@"; else aws "$@"; fi; }

AK="${AWS_ACCESS_KEY_ID:-$(aws_ configure get aws_access_key_id || true)}"
SK="${AWS_SECRET_ACCESS_KEY:-$(aws_ configure get aws_secret_access_key || true)}"
REGION="${AWS_REGION:-$(aws_ configure get region || echo us-east-1)}"
: "${AK:?no access key (set PROFILE to a configured profile, or AWS_ACCESS_KEY_ID)}"

tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT
log(){ printf '==> %s\n' "$*"; }

obj_arn="arn:aws:s3:::${BUCKET}/*"
bkt_arn="arn:aws:s3:::${BUCKET}"

# build the publisher policy doc = (resources in $1, "-" for none) UNION this bucket's ARNs
build_policy() {
  python3 - "$1" "$obj_arn" "$bkt_arn" <<'PY'
import sys, json, urllib.parse
src, obj_arn, bkt_arn = sys.argv[1:4]
doc = {"Version": "2012-10-17", "Statement": []}
if src != "-":
    raw = open(src).read().strip()
    if raw.startswith('%'):
        raw = urllib.parse.unquote(raw)
    if raw:
        doc = json.loads(raw)
aslist = lambda r: r if isinstance(r, list) else [r]
objs, bkts = set(), set()
for st in doc.get("Statement", []):
    if st.get("Sid") == "AptPublishObjectRW":
        objs |= set(aslist(st.get("Resource", [])))
    elif st.get("Sid") == "AptPublishListBucket":
        bkts |= set(aslist(st.get("Resource", [])))
objs.add(obj_arn); bkts.add(bkt_arn)
print(json.dumps({"Version": "2012-10-17", "Statement": [
    {"Sid": "AptPublishObjectRW", "Effect": "Allow",
     "Action": ["s3:PutObject", "s3:GetObject", "s3:DeleteObject"],
     "Resource": sorted(objs)},
    {"Sid": "AptPublishListBucket", "Effect": "Allow",
     "Action": "s3:ListBucket", "Resource": sorted(bkts)},
]}, indent=2))
PY
}

# exit 0 if the doc in $1 already grants this bucket, else 1
policy_covers() {
  python3 - "$1" "$obj_arn" "$bkt_arn" <<'PY'
import sys, json
doc = json.load(open(sys.argv[1])); obj, bkt = sys.argv[2], sys.argv[3]
aslist = lambda r: r if isinstance(r, list) else [r]
o, b = set(), set()
for st in doc.get("Statement", []):
    if st.get("Sid") == "AptPublishObjectRW": o |= set(aslist(st.get("Resource", [])))
    if st.get("Sid") == "AptPublishListBucket": b |= set(aslist(st.get("Resource", [])))
sys.exit(0 if (obj in o and bkt in b) else 1)
PY
}

# normalize a `get-policy-version Document` (object or url-encoded string) -> JSON file
read_policy_doc() {  # args: policy_arn  outfile
  local dv
  dv="$(aws_ iam get-policy --policy-arn "$1" --query 'Policy.DefaultVersionId' --output text)"
  aws_ iam get-policy-version --policy-arn "$1" --version-id "$dv" \
      --query 'PolicyVersion.Document' --output json \
    | python3 -c "import sys,json,urllib.parse; v=json.load(sys.stdin); v=(json.loads(urllib.parse.unquote(v)) if v.startswith('%') else json.loads(v)) if isinstance(v,str) else v; print(json.dumps(v))" \
    > "$2"
}

# ---- 1. bucket ---------------------------------------------------------------
if aws_ s3api head-bucket --bucket "$BUCKET" >/dev/null 2>&1; then
  log "bucket '$BUCKET' already exists"
else
  log "creating bucket '$BUCKET'"
  aws_ s3api create-bucket --bucket "$BUCKET" >/dev/null
fi

# ---- 2. anonymous public-read policy (GetObject only) ------------------------
cat > "$tmp/bucket-policy.json" <<JSON
{"Version":"2012-10-17","Statement":[
  {"Sid":"PublicReadGetObject","Effect":"Allow","Principal":"*",
   "Action":"s3:GetObject","Resource":"${obj_arn}"}
]}
JSON
log "applying anonymous public-read (GetObject only) policy to '$BUCKET'"
aws_ s3api put-bucket-policy --bucket "$BUCKET" --policy "file://$tmp/bucket-policy.json" >/dev/null

# ---- 3. publisher group + additive policy ------------------------------------
policy_arn=""
if [ -n "$GROUP" ]; then
  if group_arn="$(aws_ iam get-group --group-name "$GROUP" --query 'Group.Arn' --output text 2>/dev/null)"; then
    log "group '$GROUP' already exists"
  else
    log "creating group '$GROUP'"
    group_arn="$(aws_ iam create-group --group-name "$GROUP" --query 'Group.Arn' --output text)"
  fi
  acct="$(printf '%s' "$group_arn" | cut -d: -f5)"
  policy_arn="urn:ecs:iam::${acct}:policy/${POLICY}"

  if aws_ iam get-policy --policy-arn "$policy_arn" >/dev/null 2>&1; then
    read_policy_doc "$policy_arn" "$tmp/cur.json"
    if policy_covers "$tmp/cur.json"; then
      log "policy '$POLICY' already grants '$BUCKET' — no change"
    else
      nver="$(aws_ iam list-policy-versions --policy-arn "$policy_arn" --query 'length(Versions)' --output text)"
      if [ "$nver" -ge 5 ]; then
        old="$(aws_ iam list-policy-versions --policy-arn "$policy_arn" --query 'Versions[?!IsDefaultVersion]|[-1].VersionId' --output text)"
        log "policy at 5-version cap; deleting oldest non-default version ($old)"
        aws_ iam delete-policy-version --policy-arn "$policy_arn" --version-id "$old" >/dev/null
      fi
      build_policy "$tmp/cur.json" > "$tmp/policy.json"
      log "extending policy '$POLICY' to also grant '$BUCKET' (new default version)"
      aws_ iam create-policy-version --policy-arn "$policy_arn" --policy-document "file://$tmp/policy.json" --set-as-default >/dev/null
    fi
  else
    build_policy "-" > "$tmp/policy.json"
    log "creating policy '$POLICY' granting '$BUCKET'"
    aws_ iam create-policy --policy-name "$POLICY" --policy-document "file://$tmp/policy.json" >/dev/null
  fi

  if ! aws_ iam list-attached-group-policies --group-name "$GROUP" --query 'AttachedPolicies[].PolicyArn' --output text 2>/dev/null | grep -qF "$policy_arn"; then
    log "attaching policy '$POLICY' to group '$GROUP'"
    aws_ iam attach-group-policy --group-name "$GROUP" --policy-arn "$policy_arn" >/dev/null
  fi
fi

# ---- 4. user + membership + key ----------------------------------------------
akid=""
if [ -n "$IAM_USER" ]; then
  if aws_ iam get-user --user-name "$IAM_USER" >/dev/null 2>&1; then
    log "user '$IAM_USER' already exists"
  else
    log "creating user '$IAM_USER'"
    aws_ iam create-user --user-name "$IAM_USER" >/dev/null
  fi
  if aws_ iam get-group --group-name "$GROUP" --query 'Users[].UserName' --output text 2>/dev/null | grep -qw "$IAM_USER"; then
    log "user '$IAM_USER' already in group '$GROUP'"
  else
    log "adding user '$IAM_USER' to group '$GROUP'"
    aws_ iam add-user-to-group --group-name "$GROUP" --user-name "$IAM_USER" >/dev/null
  fi
  if [ "${CREATE_KEY:-0}" = "1" ]; then
    creds="$(aws_ iam create-access-key --user-name "$IAM_USER" --output json)"
    akid="$(printf '%s' "$creds" | python3 -c 'import sys,json;print(json.load(sys.stdin)["AccessKey"]["AccessKeyId"])')"
    asec="$(printf '%s' "$creds" | python3 -c 'import sys,json;print(json.load(sys.stdin)["AccessKey"]["SecretAccessKey"])')"
    # Secret is written to the file ONLY — never echoed to the terminal/transcript.
    umask 077
    cat > "./${IAM_USER}.creds" <<CREDS
# GitHub 'release' Environment secrets for the apt publisher '${IAM_USER}'.
# Store these in the repo's Environment, then delete this file.
APT_S3_ACCESS_KEY=${akid}
APT_S3_SECRET_KEY=${asec}
CREDS
    log "minted access key for '${IAM_USER}' — id=${akid}; SECRET written to ./${IAM_USER}.creds (chmod 600), NOT printed here."
  fi
fi

# ---- 5. optional abort-incomplete-multipart lifecycle (NO object expiry) -----
if [ "$ABORT_MPU_DAYS" != "0" ]; then
  printf '<LifecycleConfiguration><Rule><ID>abort-mpu</ID><Prefix></Prefix><Status>Enabled</Status><AbortIncompleteMultipartUpload><DaysAfterInitiation>%s</DaysAfterInitiation></AbortIncompleteMultipartUpload></Rule></LifecycleConfiguration>' "$ABORT_MPU_DAYS" > "$tmp/lc.xml"
  md5="$(openssl dgst -md5 -binary "$tmp/lc.xml" | openssl base64)"
  log "applying abort-incomplete-multipart lifecycle (${ABORT_MPU_DAYS}d) on '$BUCKET' (no object expiry)"
  code="$(curl -sS -o "$tmp/resp" -w '%{http_code}' -X PUT "${ENDPOINT%/}/${BUCKET}?lifecycle" \
    --aws-sigv4 "aws:amz:${REGION}:s3" --user "$AK:$SK" \
    -H "Content-MD5: $md5" -H "Content-Type: application/xml" --data-binary @"$tmp/lc.xml")"
  [ "$code" = "200" ] || { echo "lifecycle PUT failed (HTTP $code):"; cat "$tmp/resp"; echo; exit 1; }
fi

# ---- output ------------------------------------------------------------------
base="${ENDPOINT%/}/${BUCKET}"
echo
echo "================== GitHub 'release' Environment (cs-agent repo) =================="
echo "vars:"
echo "  APT_S3_ENDPOINT = ${ENDPOINT}"
echo "  APT_S3_REGION   = ${REGION}"
echo "  APT_S3_BUCKET   = ${BUCKET}"
echo "  APT_S3_PREFIX   =            # empty: the bucket root IS the repo root"
echo "secrets:"
if [ -n "$akid" ]; then
  echo "  APT_S3_ACCESS_KEY = ${akid}"
  echo "  APT_S3_SECRET_KEY = (in ./${IAM_USER}.creds — paste into the Environment, then delete the file)"
else
  echo "  APT_S3_ACCESS_KEY / APT_S3_SECRET_KEY = mint with GROUP+IAM_USER+CREATE_KEY=1, or by hand for the publisher user"
fi
echo "  GPG_PRIVATE_KEY / GPG_PASSPHRASE = the dedicated signing subkey (separate step)"
echo
echo "================== node apt source (provisioner: drop once) =================="
echo "# keyring: curl -fsSL ${base}/computestacks.gpg.asc | gpg --dearmor | tee /etc/apt/keyrings/computestacks.gpg"
echo "deb [signed-by=/etc/apt/keyrings/computestacks.gpg] ${base} ${SUITE} ${COMPONENT}"
echo "=============================================================================="
log "done. public base = ${base}"
