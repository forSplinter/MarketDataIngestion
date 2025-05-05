#!/bin/bash

echo "Create MinIO buckets"


# Initialisation de l’alias local MinIO
mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"

MINIO_BUCKETS="raw trusted enriched"
for bucket in $MINIO_BUCKETS
do
    mc mb --ignore-existing "local/$bucket"
    mc anonymous set public "local/$bucket"
    echo "Bucket $bucket created and made public"
done

echo "All buckets created"

