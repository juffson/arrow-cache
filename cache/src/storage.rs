use crate::config::Config;
use crate::config::StorageConfig;
use crate::pool::DB;
use anyhow::Ok;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::prelude::*;
use std::sync::Arc;

impl DB<()> {
    pub fn init_storages(&self, config: &Config) -> anyhow::Result<()> {
        for (name, storage_config) in &config.storages {
            self.register_storage(name, storage_config)?;
        }
        Ok(())
    }

    fn register_storage(&self, name: &str, config: &StorageConfig) -> anyhow::Result<()> {
        let object_store = object_store::aws::AmazonS3Builder::new()
            .with_access_key_id(&config.access_key_id)
            .with_secret_access_key(&config.secret_access_key)
            .with_bucket_name(&config.bucket)
            .with_region(&config.region);

        // ALIYUN OSS 必须使用 virtual_hosted_style_request，暂时这么判断
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
        // TODO: support oss:// s3://
        let url = ListingTableUrl::parse(format!("oss://{}", config.bucket))?;
        self.ctx
            .register_object_store(url.as_ref(), object_store.clone());

        let mut storages = self.registered_storages.write().unwrap();
        storages.insert(name.to_string(), object_store);

        Ok(())
    }

    pub async fn query_from_storage(&self, storage: &str, path: &str) -> anyhow::Result<DataFrame> {
        let sql = format!("SELECT * FROM '{}/{}'", storage, path);
        self.query(&sql).await
    }

    pub async fn export_to_storage_default(
        &self,
        df: DataFrame,
        location: &str,
        format: &str,
    ) -> anyhow::Result<()> {
        match format.to_lowercase().as_str() {
            "csv" => {
                let _ = df.write_csv(location, Default::default(), None).await?;
            }
            "parquet" => {
                let _ = df.write_parquet(location, Default::default(), None).await?;
            }
            _ => return Err(anyhow::anyhow!("Unsupported format: {}", format)),
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::env;
    use std::fs::File;
    use std::io::Write;
    use tempfile::tempdir;

    #[tokio::test]
    // 测试 oss 存储，目前必须是 oss
    // 需要设置 OSS_BUCKET, OSS_ACCESS_KEY, OSS_ACCESS_SECRET 三个环境变量
    // 然后执行 cargo test --test test_storage_operations -- --nocapture
    async fn test_storage_operations() -> anyhow::Result<()> {
        // 创建测试数据
        let test_data = r#"id,name,amount
1,Alice,100.50
2,Bob,200.75
3,Charlie,350.25"#;

        // 创建临时文件
        let temp_dir = tempdir()?;
        let input_path = temp_dir.path().join("test_data.csv");
        let mut file = File::create(&input_path)?;
        file.write_all(test_data.as_bytes())?;
        println!("generated temp file done, path is: {:?}", input_path);

        let bucket = env::var("OSS_BUCKET").unwrap();

        // 创建测试配置
        let mut storages = HashMap::new();
        storages.insert(
            "oss".to_string(),
            StorageConfig {
                access_key_id: env::var("OSS_ACCESS_KEY").unwrap(),
                secret_access_key: env::var("OSS_ACCESS_SECRET").unwrap(),
                bucket: bucket.clone(),
                region: "ap-east-1".to_string(),
                endpoint: Some(format!(
                    "https://{bucket}.oss-cn-hongkong.aliyuncs.com",
                    bucket = bucket
                )),
                schema: "oss".to_string(),
            },
        );
        let config = Config { storages };

        // 初始化数据库
        let db = DB::<()>::new("test_db");
        db.init_storages(&config)?;

        // 创建本地表作为初始化数据
        let sql = format!(
            r#"
            CREATE EXTERNAL TABLE local_table (
                id INT,
                name VARCHAR,
                amount DECIMAL(10,2)
            )
            STORED AS CSV
            LOCATION '{input_path}'
           ;
        "#,
            input_path = input_path.to_string_lossy()
        );
        db.query(&sql).await?;

        // 倒序一下
        let result = db
            .query("SELECT * FROM local_table ORDER BY id DESC")
            .await?;
        result.clone().show().await?;

        // 导出到新位置
        db.export_to_storage_default(
            result,
            &format!("oss://{bucket}/tests/reverse_id.csv", bucket = bucket),
            "csv",
        )
        .await?;

        // 注册 remote 表
        let remote_sql = format!(
            r#"
            CREATE EXTERNAL TABLE remote_table (
                id INT,
                name VARCHAR,
                amount DECIMAL(10,2)
            )
            STORED AS CSV
            LOCATION 'oss://{bucket}/tests/reverse_id.csv'
        "#,
            bucket = bucket
        );
        let _ = db.query(&remote_sql).await?;
        let reverse_id_result = db.query("SELECT * FROM remote_table").await?;
        reverse_id_result.clone().show().await?;
        // 验证结果
        let count = reverse_id_result.count().await?;
        assert_eq!(count, 3);

        Ok(())
    }
}
