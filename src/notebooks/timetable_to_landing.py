# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
import fnmatch
import hashlib
import logging
import os
import time
import uuid

import boto3

# force=True because Databricks pre-configures the root logger, which would make basicConfig a no-op
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", force=True)
logger = logging.getLogger("timetable_to_landing")

# COMMAND ----------

dbutils.widgets.text("s3_bucket", "")
dbutils.widgets.text("s3_object_prefix", "PPTimetable/")
dbutils.widgets.text("region", "")
dbutils.widgets.text("secret_scope", "darwin-s3-creds")
dbutils.widgets.text("landing_path", "")
dbutils.widgets.text("include_patterns", "*_v8.xml.gz,*_ref_v3.xml.gz")
dbutils.widgets.dropdown("dry_run", "false", ["true", "false"])

bucket_name = dbutils.widgets.get("s3_bucket")
object_prefix = dbutils.widgets.get("s3_object_prefix")
region = dbutils.widgets.get("region")
secret_scope = dbutils.widgets.get("secret_scope")
landing_path = dbutils.widgets.get("landing_path").rstrip("/")
include_patterns = [p.strip() for p in dbutils.widgets.get("include_patterns").split(",") if p.strip()]
dry_run = dbutils.widgets.get("dry_run") == "true"

required = {"s3_bucket": bucket_name, "region": region, "landing_path": landing_path}
unset = [name for name, value in required.items() if not value]
if unset:
    raise ValueError(f"these widgets must be set: {', '.join(unset)}")
if not include_patterns:
    raise ValueError("include_patterns must list at least one pattern")

# COMMAND ----------

access_key = dbutils.secrets.get(secret_scope, "access_key_id")
secret_access_key = dbutils.secrets.get(secret_scope, "secret_access_key")

s3 = boto3.resource("s3", region_name=region, aws_access_key_id=access_key, aws_secret_access_key=secret_access_key)
bucket = s3.Bucket(bucket_name)

# COMMAND ----------

CHUNK_SIZE = 1024 * 1024
MAX_ATTEMPTS = 3
RETRY_BACKOFF_SECONDS = 5
STALE_TMP_AGE_SECONDS = 6 * 3600


def is_wanted(key):
    return any(fnmatch.fnmatch(os.path.basename(key), p) for p in include_patterns)


def sweep_stale_temps(root):
    """Delete leftover temp files from runs that were killed before their finally block could run."""
    removed = 0
    cutoff = time.time() - STALE_TMP_AGE_SECONDS
    for dirpath, _, filenames in os.walk(root):
        for name in filenames:
            if not (name.startswith(".") and name.endswith(".tmp")):
                continue
            path = os.path.join(dirpath, name)
            try:
                if os.path.getmtime(path) < cutoff:
                    os.remove(path)
                    removed += 1
            except OSError as e:
                logger.warning("could not remove stale temp %s: %r", path, e)
    if removed:
        logger.info("removed %d stale temp file(s)", removed)


def copy_object(obj, dest_path):
    """Stream one S3 object to dest_path, verifying its MD5 against the ETag before it becomes visible."""
    dest_dir, dest_name = os.path.split(dest_path)
    os.makedirs(dest_dir, exist_ok=True)
    # leading "." hides the in-progress file from Spark / Auto Loader readers;
    # same directory as the destination so the final rename stays atomic
    tmp_path = os.path.join(dest_dir, f".{dest_name}.{uuid.uuid4().hex}.tmp")

    md5 = hashlib.md5()
    try:
        body = obj.get()["Body"]
        with open(tmp_path, "wb") as f:
            for chunk in body.iter_chunks(CHUNK_SIZE):
                f.write(chunk)
                md5.update(chunk)

        etag = obj.e_tag.strip('"')
        if "-" in etag:
            # multipart upload: the ETag is not a plain MD5, so it can't be used for verification
            logger.warning("%s: multipart ETag, skipping checksum verification", obj.key)
        elif md5.hexdigest() != etag:
            raise ValueError(f"checksum mismatch: md5={md5.hexdigest()} etag={etag}")

        os.replace(tmp_path, dest_path)
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


def copy_object_with_retries(obj, dest_path):
    """Retry a failed copy: a mid-stream read error or a corrupted transfer is usually transient."""
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            copy_object(obj, dest_path)
            return
        except Exception as e:
            if attempt == MAX_ATTEMPTS:
                raise
            delay = RETRY_BACKOFF_SECONDS * attempt
            logger.warning("attempt %d/%d failed for %s (%r), retrying in %ds",
                           attempt, MAX_ATTEMPTS, obj.key, e, delay)
            time.sleep(delay)

# COMMAND ----------

os.makedirs(landing_path, exist_ok=True)
sweep_stale_temps(landing_path)

listed = matched = skipped = copied = 0
failures = []

for obj in bucket.objects.filter(Prefix=object_prefix):
    listed += 1
    if not is_wanted(obj.key):
        continue
    matched += 1

    dest_path = f"{landing_path}/{obj.key}"
    if os.path.exists(dest_path):
        skipped += 1
        continue

    if dry_run:
        logger.info("would copy %s (%d bytes)", obj.key, obj.size)
        copied += 1
        continue

    try:
        copy_object_with_retries(obj, dest_path)
        copied += 1
        logger.info("copied %s (%d bytes)", obj.key, obj.size)
    except Exception:
        failures.append(obj.key)
        logger.exception("failed %s", obj.key)

logger.info("dry_run=%s listed=%d matched=%d skipped=%d copied=%d failed=%d",
            dry_run, listed, matched, skipped, copied, len(failures))

if failures:
    raise RuntimeError(f"{len(failures)} file(s) failed to copy: " + ", ".join(failures))
