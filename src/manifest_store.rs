// Copyright 2026 Infino AI, Inc. All rights reserved.
//! Manifest Store: object storage backend for encrypted K8s manifests.
//!
//! Provides a cloud-agnostic interface for storing, retrieving, and deleting
//! encrypted manifest files using the `object_store` crate. Supports AWS S3,
//! Google Cloud Storage, and Azure Blob Storage via ambient credentials.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::TryStreamExt;
use log::{debug, info};
use object_store::aws::AmazonS3Builder;
use object_store::azure::MicrosoftAzureBuilder;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectStorePath;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, RetryConfig};
use thiserror::Error;

const NUM_RETRIES: usize = 3;
const RETRY_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Error)]
pub enum ManifestStoreError {
    #[error("environment variable {0} is required but not set")]
    MissingEnvVar(String),

    #[error("unsupported cloud provider: {0} (expected aws, gcp, azure, or local)")]
    UnsupportedProvider(String),

    #[error("object store error: {0}")]
    ObjectStore(#[from] object_store::Error),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Debug, Clone)]
pub enum CloudProvider {
    Aws,
    Gcp,
    Azure,
    Local,
}

impl CloudProvider {
    pub fn from_str(s: &str) -> Result<Self, ManifestStoreError> {
        match s.to_lowercase().as_str() {
            "aws" => Ok(Self::Aws),
            "gcp" => Ok(Self::Gcp),
            "azure" => Ok(Self::Azure),
            "local" => Ok(Self::Local),
            other => Err(ManifestStoreError::UnsupportedProvider(other.to_string())),
        }
    }
}

pub struct ManifestStore {
    store: Arc<dyn ObjectStore>,
}

impl ManifestStore {
    /// Build a ManifestStore from environment variables.
    ///
    /// Required env vars:
    /// - `DEPLOYMENT_MANIFEST_BUCKET` -- bucket/container name
    /// - `CLOUD_PROVIDER` -- one of "aws", "gcp", "azure", "local"
    ///
    /// Cloud-specific credentials are picked up automatically by each builder's
    /// `from_env()` method (e.g. AWS_ACCESS_KEY_ID, GOOGLE_APPLICATION_CREDENTIALS, etc).
    pub fn from_env() -> Result<Self, ManifestStoreError> {
        let bucket = required_env("DEPLOYMENT_MANIFEST_BUCKET")?;
        let provider_str = required_env("CLOUD_PROVIDER")?;
        let provider = CloudProvider::from_str(&provider_str)?;

        let retry_config = RetryConfig {
            max_retries: NUM_RETRIES,
            retry_timeout: RETRY_TIMEOUT,
            ..RetryConfig::default()
        };

        let store: Arc<dyn ObjectStore> = match provider {
            CloudProvider::Aws => {
                let builder = AmazonS3Builder::from_env()
                    .with_retry(retry_config)
                    .with_bucket_name(&bucket)
                    .build()?;
                Arc::new(builder)
            }
            CloudProvider::Gcp => {
                let builder = GoogleCloudStorageBuilder::from_env()
                    .with_retry(retry_config)
                    .with_bucket_name(&bucket)
                    .build()?;
                Arc::new(builder)
            }
            CloudProvider::Azure => {
                let builder = MicrosoftAzureBuilder::from_env()
                    .with_retry(retry_config)
                    .with_container_name(&bucket)
                    .build()?;
                Arc::new(builder)
            }
            CloudProvider::Local => {
                let local_store = LocalFileSystem::new_with_prefix(&bucket)?;
                Arc::new(local_store)
            }
        };

        Ok(Self { store })
    }

    /// Create a ManifestStore backed by the local filesystem at `root_dir`.
    /// Useful for testing and local development.
    pub fn new_local(root_dir: &Path) -> Result<Self, ManifestStoreError> {
        let store = LocalFileSystem::new_with_prefix(root_dir)?;
        Ok(Self {
            store: Arc::new(store),
        })
    }

    pub fn object_path(cloud_provider: &str, environment: &str, name: &str) -> ObjectStorePath {
        ObjectStorePath::from(format!("{cloud_provider}/{environment}/{name}"))
    }

    /// Upload a local file to the object store.
    pub async fn put(
        &self,
        cloud_provider: &str,
        environment: &str,
        name: &str,
        file_path: &Path,
    ) -> Result<(), ManifestStoreError> {
        let data = tokio::fs::read(file_path).await?;
        let path = Self::object_path(cloud_provider, environment, name);
        info!("Uploading manifest to {}", path);

        self.store
            .put(&path, PutPayload::from(Bytes::from(data)))
            .await?;

        debug!("Upload complete: {}", path);
        Ok(())
    }

    /// Download a manifest from the object store to a local file.
    pub async fn get(
        &self,
        cloud_provider: &str,
        environment: &str,
        name: &str,
        output_path: &Path,
    ) -> Result<(), ManifestStoreError> {
        let path = Self::object_path(cloud_provider, environment, name);
        info!("Downloading manifest from {}", path);

        let result = self.store.get(&path).await?;
        let data = result.bytes().await?;
        tokio::fs::write(output_path, &data).await?;

        debug!("Download complete: {} -> {}", path, output_path.display());
        Ok(())
    }

    /// Delete a manifest from the object store.
    pub async fn delete(
        &self,
        cloud_provider: &str,
        environment: &str,
        name: &str,
    ) -> Result<(), ManifestStoreError> {
        let path = Self::object_path(cloud_provider, environment, name);
        info!("Deleting manifest at {}", path);

        self.store.delete(&path).await?;

        debug!("Delete complete: {}", path);
        Ok(())
    }

    /// List all manifests under a given cloud_provider/environment prefix.
    pub async fn list(
        &self,
        cloud_provider: &str,
        environment: &str,
    ) -> Result<Vec<String>, ManifestStoreError> {
        let prefix = ObjectStorePath::from(format!("{cloud_provider}/{environment}/"));
        info!("Listing manifests under {}", prefix);

        let mut names = Vec::new();
        let mut stream = self.store.list(Some(&prefix));
        while let Some(meta) = stream.try_next().await? {
            if let Some(file_name) = meta.location.filename() {
                names.push(file_name.to_string());
            }
        }

        debug!("Found {} manifests under {}", names.len(), prefix);
        Ok(names)
    }
}

fn required_env(key: &str) -> Result<String, ManifestStoreError> {
    std::env::var(key).map_err(|_| ManifestStoreError::MissingEnvVar(key.to_string()))
}
