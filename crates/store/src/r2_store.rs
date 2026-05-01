use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_sdk_s3::config::Region;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::OnceCell;

use crate::object_store::{ObjectMetadata, ObjectStore, RemoteObject};

#[derive(Clone, Debug)]
pub struct R2Config {
    pub account_id: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub bucket: String,
    pub endpoint_url: Option<String>,
    pub region: String,
    pub multipart_threshold_bytes: u64,
    pub multipart_part_size_bytes: usize,
}

impl R2Config {
    pub fn from_env() -> Result<Self> {
        let account_id =
            std::env::var("LEDGER_R2_ACCOUNT_ID").context("LEDGER_R2_ACCOUNT_ID missing")?;
        let access_key_id =
            std::env::var("LEDGER_R2_ACCESS_KEY_ID").context("LEDGER_R2_ACCESS_KEY_ID missing")?;
        let secret_access_key = std::env::var("LEDGER_R2_SECRET_ACCESS_KEY")
            .context("LEDGER_R2_SECRET_ACCESS_KEY missing")?;
        let bucket = std::env::var("LEDGER_R2_BUCKET").context("LEDGER_R2_BUCKET missing")?;
        let endpoint_url = std::env::var("LEDGER_R2_ENDPOINT_URL").ok();
        Ok(Self {
            account_id,
            access_key_id,
            secret_access_key,
            bucket,
            endpoint_url,
            region: std::env::var("LEDGER_R2_REGION").unwrap_or_else(|_| "auto".to_string()),
            multipart_threshold_bytes: std::env::var("LEDGER_R2_MULTIPART_THRESHOLD_BYTES")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(1024 * 1024 * 1024),
            multipart_part_size_bytes: std::env::var("LEDGER_R2_MULTIPART_PART_SIZE_BYTES")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(64 * 1024 * 1024),
        })
    }

    pub fn endpoint(&self) -> String {
        self.endpoint_url
            .clone()
            .unwrap_or_else(|| format!("https://{}.r2.cloudflarestorage.com", self.account_id))
    }
}

#[derive(Clone)]
pub struct R2ObjectStore {
    inner: Arc<R2ObjectStoreInner>,
}

struct R2ObjectStoreInner {
    config: R2Config,
    client: OnceCell<Client>,
}

impl R2ObjectStore {
    pub async fn new(config: R2Config) -> Result<Self> {
        Ok(Self {
            inner: Arc::new(R2ObjectStoreInner {
                config,
                client: OnceCell::new(),
            }),
        })
    }

    async fn client(&self) -> Result<&Client> {
        self.inner
            .client
            .get_or_try_init(|| async { Self::build_client(&self.inner.config).await })
            .await
    }

    async fn build_client(config: &R2Config) -> Result<Client> {
        let creds = Credentials::new(
            config.access_key_id.clone(),
            config.secret_access_key.clone(),
            None,
            None,
            "ledger-r2",
        );
        let sdk_config = aws_config::defaults(BehaviorVersion::latest())
            .endpoint_url(config.endpoint())
            .region(Region::new(config.region.clone()))
            .credentials_provider(creds)
            .load()
            .await;
        Ok(Client::new(&sdk_config))
    }

    fn metadata_map(metadata: &ObjectMetadata) -> HashMap<String, String> {
        let mut map = metadata.user_metadata.clone();
        map.insert("sha256".to_string(), metadata.sha256.clone());
        map.insert("format".to_string(), metadata.format.clone());
        map.insert(
            "schema-version".to_string(),
            metadata.schema_version.to_string(),
        );
        map
    }

    async fn put_path_single(
        &self,
        key: &str,
        path: &Path,
        metadata: &ObjectMetadata,
    ) -> Result<RemoteObject> {
        let client = self.client().await?.clone();
        let body = ByteStream::from_path(path)
            .await
            .with_context(|| format!("opening {} for upload", path.display()))?;
        let mut req = client
            .put_object()
            .bucket(self.bucket())
            .key(key)
            .body(body)
            .set_metadata(Some(Self::metadata_map(metadata)));
        if let Some(ct) = &metadata.content_type {
            req = req.content_type(ct);
        }
        let out = req
            .send()
            .await
            .with_context(|| format!("uploading s3://{}/{}", self.bucket(), key))?;
        Ok(RemoteObject {
            bucket: self.bucket().to_string(),
            key: key.to_string(),
            size_bytes: metadata.size_bytes,
            sha256: Some(metadata.sha256.clone()),
            etag: out.e_tag().map(|s| s.to_string()),
            metadata: Self::metadata_map(metadata),
        })
    }

    async fn put_path_multipart(
        &self,
        key: &str,
        path: &Path,
        metadata: &ObjectMetadata,
    ) -> Result<RemoteObject> {
        use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};

        let client = self.client().await?.clone();
        let bucket = self.bucket().to_string();

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .set_metadata(Some(Self::metadata_map(metadata)))
            .send()
            .await
            .with_context(|| format!("creating multipart upload for s3://{}/{}", bucket, key))?;
        let upload_id = create
            .upload_id()
            .ok_or_else(|| anyhow!("R2 did not return upload id"))?
            .to_string();

        let mut file = tokio::fs::File::open(path).await?;
        let mut part_number = 1;
        let mut completed = Vec::new();
        let mut buf = vec![0_u8; self.inner.config.multipart_part_size_bytes];

        loop {
            let n = file.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            let body = ByteStream::from(buf[..n].to_vec());
            let part = client
                .upload_part()
                .bucket(&bucket)
                .key(key)
                .upload_id(&upload_id)
                .part_number(part_number)
                .body(body)
                .send()
                .await;
            match part {
                Ok(out) => {
                    completed.push(
                        CompletedPart::builder()
                            .part_number(part_number)
                            .set_e_tag(out.e_tag().map(|s| s.to_string()))
                            .build(),
                    );
                }
                Err(err) => {
                    let _ = client
                        .abort_multipart_upload()
                        .bucket(&bucket)
                        .key(key)
                        .upload_id(&upload_id)
                        .send()
                        .await;
                    return Err(err)
                        .with_context(|| format!("uploading multipart part {part_number}"));
                }
            }
            part_number += 1;
        }

        let completed_upload = CompletedMultipartUpload::builder()
            .set_parts(Some(completed))
            .build();
        let out = client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .multipart_upload(completed_upload)
            .send()
            .await
            .with_context(|| format!("completing multipart upload for s3://{}/{}", bucket, key))?;

        Ok(RemoteObject {
            bucket,
            key: key.to_string(),
            size_bytes: metadata.size_bytes,
            sha256: Some(metadata.sha256.clone()),
            etag: out.e_tag().map(|s| s.to_string()),
            metadata: Self::metadata_map(metadata),
        })
    }
}

#[async_trait]
impl ObjectStore for R2ObjectStore {
    async fn put_path(
        &self,
        key: &str,
        path: &Path,
        metadata: &ObjectMetadata,
    ) -> Result<RemoteObject> {
        if metadata.size_bytes as u64 >= self.inner.config.multipart_threshold_bytes {
            self.put_path_multipart(key, path, metadata).await
        } else {
            self.put_path_single(key, path, metadata).await
        }
    }

    async fn put_bytes(
        &self,
        key: &str,
        bytes: &[u8],
        metadata: &ObjectMetadata,
    ) -> Result<RemoteObject> {
        let client = self.client().await?.clone();
        let mut req = client
            .put_object()
            .bucket(self.bucket())
            .key(key)
            .body(ByteStream::from(bytes.to_vec()))
            .set_metadata(Some(Self::metadata_map(metadata)));
        if let Some(ct) = &metadata.content_type {
            req = req.content_type(ct);
        }
        let out = req
            .send()
            .await
            .with_context(|| format!("uploading s3://{}/{}", self.bucket(), key))?;
        Ok(RemoteObject {
            bucket: self.bucket().to_string(),
            key: key.to_string(),
            size_bytes: bytes.len() as i64,
            sha256: Some(metadata.sha256.clone()),
            etag: out.e_tag().map(|s| s.to_string()),
            metadata: Self::metadata_map(metadata),
        })
    }

    async fn get_to_path(&self, key: &str, dest: &Path) -> Result<RemoteObject> {
        let client = self.client().await?.clone();
        if let Some(parent) = dest.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let tmp = crate::local_store::tmp_path(dest);
        let out = client
            .get_object()
            .bucket(self.bucket())
            .key(key)
            .send()
            .await?;
        let metadata = out.metadata().cloned().unwrap_or_default();
        let sha256 = metadata.get("sha256").cloned();
        let mut reader = out.body.into_async_read();
        let mut file = tokio::fs::File::create(&tmp).await?;
        tokio::io::copy(&mut reader, &mut file).await?;
        file.flush().await?;
        file.sync_all().await?;
        drop(file);
        tokio::fs::rename(&tmp, dest).await?;
        let size = tokio::fs::metadata(dest).await?.len() as i64;
        Ok(RemoteObject {
            bucket: self.bucket().to_string(),
            key: key.to_string(),
            size_bytes: size,
            sha256,
            etag: None,
            metadata,
        })
    }

    async fn head(&self, key: &str) -> Result<Option<RemoteObject>> {
        let client = self.client().await?.clone();
        let res = client
            .head_object()
            .bucket(self.bucket())
            .key(key)
            .send()
            .await;
        match res {
            Ok(out) => {
                let metadata = out.metadata().cloned().unwrap_or_default();
                Ok(Some(RemoteObject {
                    bucket: self.bucket().to_string(),
                    key: key.to_string(),
                    size_bytes: out.content_length().unwrap_or_default(),
                    sha256: metadata.get("sha256").cloned(),
                    etag: out.e_tag().map(|s| s.to_string()),
                    metadata,
                }))
            }
            Err(err) => {
                if err
                    .as_service_error()
                    .is_some_and(|service_error| service_error.is_not_found())
                {
                    Ok(None)
                } else {
                    Err(err).with_context(|| format!("HEAD s3://{}/{}", self.bucket(), key))
                }
            }
        }
    }

    fn bucket(&self) -> &str {
        &self.inner.config.bucket
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dummy_config() -> R2Config {
        R2Config {
            account_id: "account".to_string(),
            access_key_id: "access".to_string(),
            secret_access_key: "secret".to_string(),
            bucket: "bucket".to_string(),
            endpoint_url: Some("https://example.invalid".to_string()),
            region: "auto".to_string(),
            multipart_threshold_bytes: 1024,
            multipart_part_size_bytes: 1024,
        }
    }

    #[tokio::test]
    async fn r2_store_construction_is_lazy() {
        let store = R2ObjectStore::new(dummy_config()).await.unwrap();

        assert_eq!(store.bucket(), "bucket");
    }
}
