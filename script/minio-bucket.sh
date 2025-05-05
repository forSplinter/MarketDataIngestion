echo "create minio bucket"
source .env

mc alias set local "${MINIO_HOST}" "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}"

MINIO_BUCKETS=("raw", "trusted", "enriched")

for bucket in "${MINIO_BUCKETS[@]}"
do
    mc mb "local/${bucket}" || echo "Bucket ${bucket} already exists"
    echo "Applying policy to bucket ${bucket}"
    mc versioning enable "local/${bucket}"
    echo "Bucket ${bucket} created"
done
echo "All buckets created"