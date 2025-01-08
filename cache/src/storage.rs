use crate::config::Config;
use crate::config::StorageConfig;
use crate::pool::DB;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;

impl DB<()> {
    fn init_storages(&self, config: &Config) -> anyhow::Result<()> {
        for (name, storage_config) in &config.storages {
            self.register_storage(name, storage_config)?;
        }
        Ok(())
    }

    pub fn register_storage(&self, name: &str, config: &StorageConfig) -> anyhow::Result<()> {
        let object_store = object_store::aws::AmazonS3Builder::new()
            .with_access_key_id(&config.access_key_id)
            .with_secret_access_key(&config.secret_access_key)
            .with_bucket_name(&config.bucket)
            .with_region(&config.region);

        // ALIYUN OSS 必须使用 virtual_hosted_style_request
        let object_store = if let Some(endpoint) = &config.endpoint {
            object_store
                .with_endpoint(endpoint)
                .with_virtual_hosted_style_request(true)
                .with_allow_http(true)
                .build()?
        } else {
            object_store.build()?
        };

        let object_store = Arc::new(object_store);
        let url = Url::parse(&format!("s3://{}", config.bucket))?;
        self.ctx.register_object_store(&url, object_store.clone());

        let mut storages = self.registered_storages.write().unwrap();
        storages.insert(name.to_string(), object_store);

        Ok(())
    }

    pub async fn query_from_storage(&self, storage: &str, path: &str) -> anyhow::Result<DataFrame> {
        let sql = format!("SELECT * FROM '{}/{}'", storage, path);
        self.query(&sql).await
    }

    pub async fn export_to_storage(
        &self,
        df: DataFrame,
        storage: &str,
        path: &str,
        format: &str,
    ) -> anyhow::Result<()> {
        let location = format!("{}/{}", storage, path);
        match format.to_lowercase().as_str() {
            "csv" => df.write_csv(&location).await?,
            "parquet" => df.write_parquet(&location, None).await?,
            _ => return Err(anyhow::anyhow!("Unsupported format: {}", format)),
        }
        Ok(())
    }
}
