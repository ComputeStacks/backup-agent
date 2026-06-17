#!/usr/bin/env bash
# Provision an export bucket on UpCloud Managed Object Storage (Dell ECS backend).
# Idempotent and additive — safe to re-run.
#
# Given a bucket (and optionally a group and user) it will:
#   1. create the BUCKET if absent
#   2. if GROUP is set: create it if absent, and ensure a group-managed policy grants
#      export access to BUCKET. This is ADDITIVE — if the policy already exists it gains
#      BUCKET via a NEW policy version, preserving access to every bucket already
#      attached (the policy is never overwritten / no other bucket loses access).
#   3. if USER is set: create it if absent and add it to GROUP (so every group member,
#      including USER, can access all the group's buckets).
#   4. apply the 1-day auto-delete lifecycle to BUCKET (unless LIFECYCLE=0).
#
# Per-bucket access granted by the group policy (least privilege for the export agent):
#   PutObject, GetObject, AbortMultipartUpload, ListMultipartUploadParts on
#   <bucket>/<prefix>* ; ListBucketMultipartUploads on <bucket>.
#
# Run with an account/owner key (the default CLI profile) — NOT the scoped agent user.
#
# Usage:
#   BUCKET=team-exports GROUP=team USER=team-agent \
#   ENDPOINT=https://fra1.restore.cldprs.nl ./setup-export-bucket.sh
#
# Options (env): PREFIX=exports/  POLICY=<group>-export  LIFECYCLE=1  CREATE_KEY=0
#   The script always prints a paste-ready agent.yml (backups.export.s3) block at the
#   end. CREATE_KEY=1 mints an access key for USER, fills it into that YAML, and also
#   writes the YAML to ./<user>.creds (chmod 600).
set -euo pipefail

: "${BUCKET:?set BUCKET}"
: "${ENDPOINT:?set ENDPOINT, e.g. https://fra1.restore.cldprs.nl}"
: "${PREFIX:=exports/}"
: "${LIFECYCLE:=1}"
GROUP="${GROUP:-}"
USER="${USER:-}"
if [ -n "$USER" ] && [ -z "$GROUP" ]; then echo "USER requires GROUP (the group to add it to)"; exit 2; fi
POLICY="${POLICY:-${GROUP}-export}"
AK="${AWS_ACCESS_KEY_ID:-$(aws configure get aws_access_key_id)}"
SK="${AWS_SECRET_ACCESS_KEY:-$(aws configure get aws_secret_access_key)}"
REGION="${AWS_REGION:-$(aws configure get region || echo us-east-1)}"
: "${AK:?no access key (configure the default profile or set AWS_ACCESS_KEY_ID)}"

tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT
log(){ printf '==> %s\n' "$*"; }

obj_arn="arn:aws:s3:::${BUCKET}/${PREFIX}*"
bkt_arn="arn:aws:s3:::${BUCKET}"

# build a full policy doc = (resources in $1, "-" for none) UNION the bucket's ARNs
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
    if st.get("Sid") == "ExportObjectReadWrite":
        objs |= set(aslist(st.get("Resource", [])))
    elif st.get("Sid") == "ExportListBucketMultipart":
        bkts |= set(aslist(st.get("Resource", [])))
objs.add(obj_arn); bkts.add(bkt_arn)
print(json.dumps({"Version": "2012-10-17", "Statement": [
    {"Sid": "ExportObjectReadWrite", "Effect": "Allow",
     "Action": ["s3:PutObject", "s3:GetObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"],
     "Resource": sorted(objs)},
    {"Sid": "ExportListBucketMultipart", "Effect": "Allow",
     "Action": "s3:ListBucketMultipartUploads", "Resource": sorted(bkts)},
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
    if st.get("Sid") == "ExportObjectReadWrite": o |= set(aslist(st.get("Resource", [])))
    if st.get("Sid") == "ExportListBucketMultipart": b |= set(aslist(st.get("Resource", [])))
sys.exit(0 if (obj in o and bkt in b) else 1)
PY
}

# normalize a `get-policy-version Document` (object or url-encoded string) -> JSON file
read_policy_doc() {  # args: policy_arn  outfile
  local dv
  dv="$(aws iam get-policy --policy-arn "$1" --query 'Policy.DefaultVersionId' --output text)"
  aws iam get-policy-version --policy-arn "$1" --version-id "$dv" \
      --query 'PolicyVersion.Document' --output json \
    | python3 -c "import sys,json,urllib.parse; v=json.load(sys.stdin); v=(json.loads(urllib.parse.unquote(v)) if v.startswith('%') else json.loads(v)) if isinstance(v,str) else v; print(json.dumps(v))" \
    > "$2"
}

# ---- 1. bucket ---------------------------------------------------------------
if aws s3api head-bucket --bucket "$BUCKET" >/dev/null 2>&1; then
  log "bucket '$BUCKET' already exists"
else
  log "creating bucket '$BUCKET'"
  aws s3api create-bucket --bucket "$BUCKET" >/dev/null
fi

# ---- 2. group + additive policy ----------------------------------------------
if [ -n "$GROUP" ]; then
  if group_arn="$(aws iam get-group --group-name "$GROUP" --query 'Group.Arn' --output text 2>/dev/null)"; then
    log "group '$GROUP' already exists"
  else
    log "creating group '$GROUP'"
    group_arn="$(aws iam create-group --group-name "$GROUP" --query 'Group.Arn' --output text)"
  fi
  acct="$(printf '%s' "$group_arn" | cut -d: -f5)"
  policy_arn="urn:ecs:iam::${acct}:policy/${POLICY}"

  if aws iam get-policy --policy-arn "$policy_arn" >/dev/null 2>&1; then
    read_policy_doc "$policy_arn" "$tmp/cur.json"
    if policy_covers "$tmp/cur.json"; then
      log "policy '$POLICY' already grants '$BUCKET' — no change"
    else
      nver="$(aws iam list-policy-versions --policy-arn "$policy_arn" --query 'length(Versions)' --output text)"
      if [ "$nver" -ge 5 ]; then
        old="$(aws iam list-policy-versions --policy-arn "$policy_arn" --query 'Versions[?!IsDefaultVersion]|[-1].VersionId' --output text)"
        log "policy at 5-version cap; deleting oldest non-default version ($old)"
        aws iam delete-policy-version --policy-arn "$policy_arn" --version-id "$old" >/dev/null
      fi
      build_policy "$tmp/cur.json" > "$tmp/policy.json"
      log "extending policy '$POLICY' to also grant '$BUCKET' (new default version)"
      aws iam create-policy-version --policy-arn "$policy_arn" --policy-document "file://$tmp/policy.json" --set-as-default >/dev/null
    fi
  else
    build_policy "-" > "$tmp/policy.json"
    log "creating policy '$POLICY' granting '$BUCKET'"
    aws iam create-policy --policy-name "$POLICY" --policy-document "file://$tmp/policy.json" >/dev/null
  fi

  if ! aws iam list-attached-group-policies --group-name "$GROUP" --query 'AttachedPolicies[].PolicyArn' --output text 2>/dev/null | grep -qF "$policy_arn"; then
    log "attaching policy '$POLICY' to group '$GROUP'"
    aws iam attach-group-policy --group-name "$GROUP" --policy-arn "$policy_arn" >/dev/null
  fi
fi

# ---- 3. user + membership ----------------------------------------------------
if [ -n "$USER" ]; then
  if aws iam get-user --user-name "$USER" >/dev/null 2>&1; then
    log "user '$USER' already exists"
  else
    log "creating user '$USER'"
    aws iam create-user --user-name "$USER" >/dev/null
  fi
  if aws iam get-group --group-name "$GROUP" --query 'Users[].UserName' --output text 2>/dev/null | grep -qw "$USER"; then
    log "user '$USER' already in group '$GROUP'"
  else
    log "adding user '$USER' to group '$GROUP'"
    aws iam add-user-to-group --group-name "$GROUP" --user-name "$USER" >/dev/null
  fi
fi

# ---- 4. lifecycle ------------------------------------------------------------
if [ "$LIFECYCLE" = "1" ]; then
  printf '<LifecycleConfiguration><Rule><ID>expire-exports</ID><Prefix>%s</Prefix><Status>Enabled</Status><Expiration><Days>1</Days></Expiration><AbortIncompleteMultipartUpload><DaysAfterInitiation>1</DaysAfterInitiation></AbortIncompleteMultipartUpload></Rule></LifecycleConfiguration>' "$PREFIX" > "$tmp/lc.xml"
  md5="$(openssl dgst -md5 -binary "$tmp/lc.xml" | openssl base64)"
  log "applying 1-day lifecycle on '$BUCKET' (prefix '$PREFIX')"
  code="$(curl -sS -o "$tmp/resp" -w '%{http_code}' -X PUT "${ENDPOINT%/}/${BUCKET}?lifecycle" \
    --aws-sigv4 "aws:amz:${REGION}:s3" --user "$AK:$SK" \
    -H "Content-MD5: $md5" -H "Content-Type: application/xml" --data-binary @"$tmp/lc.xml")"
  [ "$code" = "200" ] || { echo "lifecycle PUT failed (HTTP $code):"; cat "$tmp/resp"; echo; exit 1; }
fi

# ---- summary -----------------------------------------------------------------
echo
if [ -n "${policy_arn:-}" ]; then
  log "buckets granted by group policy '$POLICY':"
  read_policy_doc "$policy_arn" "$tmp/final.json"
  python3 -c "import json;d=json.load(open('$tmp/final.json'));print('\n'.join('  - '+r for r in sorted(d['Statement'][1]['Resource'])))"
fi

# ---- paste-ready agent.yml ---------------------------------------------------
akid_yaml="REPLACE_WITH_ACCESS_KEY"
asec_yaml="REPLACE_WITH_SECRET_KEY"
note="# credentials below are placeholders — mint a key with:"
note2="#   aws iam create-access-key --user-name ${USER:-<export-user>}   (or re-run this with CREATE_KEY=1)"
if [ -n "$USER" ] && [ "${CREATE_KEY:-0}" = "1" ]; then
  creds="$(aws iam create-access-key --user-name "$USER" --output json)"
  akid_yaml="$(printf '%s' "$creds" | python3 -c 'import sys,json;print(json.load(sys.stdin)["AccessKey"]["AccessKeyId"])')"
  asec_yaml="$(printf '%s' "$creds" | python3 -c 'import sys,json;print(json.load(sys.stdin)["AccessKey"]["SecretAccessKey"])')"
  note="# access key minted for '${USER}' — the secret is shown ONLY here (and ./${USER}.creds); store it now."
  note2=""
fi

yaml="$(cat <<YAML
backups:
  export:
    workers: 1
    tar_filter: "gzip"
    timeout_sec: 14400
    s3:
      endpoint: "${ENDPOINT}"
      region: "${REGION}"
      bucket: "${BUCKET}"
      prefix: "${PREFIX}"
      access_key: "${akid_yaml}"
      secret_key: "${asec_yaml}"
      force_path_style: false
      part_size_mb: 64
      concurrency: 4
      sse: "AES256"
      default_ttl_sec: 21600
      max_ttl_sec: 86400
YAML
)"

echo
echo "================== agent.yml  (paste into the node's config) =================="
echo "$note"
[ -n "$note2" ] && echo "$note2"
echo "$yaml"
echo "==============================================================================="
if [ -n "$USER" ] && [ "${CREATE_KEY:-0}" = "1" ]; then
  umask 077; printf '%s\n' "$yaml" > "./${USER}.creds"
  echo "(also written to ./${USER}.creds — chmod 600; delete after copying onto the node)"
fi
log "done."
