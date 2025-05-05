#!/bin/bash

echo "Create MinIO buckets"

# Charger les variables d’environnement

# Initialisation de l’alias local MinIO
mc alias set local "$MINIO_HOST" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"

# Création des buckets listés dans MINIO_BUCKETS
for bucket in $MINIO_BUCKETS
do
    mc mb --ignore-existing "local/$bucket"
    mc anonymous set public "local/$bucket"
    echo "Bucket $bucket created and made public"
done

echo "All buckets created"

