use crate::config::Config;
use crate::config::StorageConfig;
use crate::pool::StorageEntry;
use crate::pool::DB;
use anyhow::Context;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::prelude::*;
use std::sync::Arc;

impl DB<()> {
    pub fn init_storages(&self, config: Config) -> anyhow::Result<()> {
        for (name, storage_config) in config.storages {
            self.register_storage(&name, storage_config)?;
        }
        Ok(())
    }

    fn register_storage(&self, name: &str, config: StorageConfig) -> anyhow::Result<()> {
        let mut object_store = object_store::aws::AmazonS3Builder::new()
            .with_access_key_id(&config.access_key)
            .with_secret_access_key(&config.access_secret)
            .with_bucket_name(&config.bucket)
            .with_allow_http(true)
            .with_region(&config.region);

        let schema = config.schema.clone();

        if let Some(endpoint) = &config.endpoint {
            object_store = object_store.with_endpoint(endpoint);
        }

        // TODO: oss may using const or enum
        // 注意 virtual_hosted_style_request 的 endpoint 是 https://{bucket}.oss-cn-hongkong.aliyuncs.com
        if schema == "oss" {
            object_store = object_store.with_virtual_hosted_style_request(true)
        }
        let object_store = Arc::new(object_store.build()?);

        let url = ListingTableUrl::parse(format!("{schema}://{}", config.bucket))?;
        self.ctx
            .register_object_store(url.as_ref(), object_store.clone());

        let mut storages = self.registered_storages.write().unwrap();
        storages.insert(
            name.to_string(),
            StorageEntry {
                store: object_store,
                config,
            },
        );

        Ok(())
    }

    pub async fn query_from_storage(&self, storage: &str, path: &str) -> anyhow::Result<DataFrame> {
        let sql = format!("SELECT * FROM '{}/{}'", storage, path);
        self.query(&sql).await
    }

    pub async fn export_to_storage(
        &self,
        df: DataFrame,
        storage_name: &str,
        path: &str,
        format: &str,
    ) -> anyhow::Result<()> {
        let (schema, bucket, path) = {
            let storages = self.registered_storages.read().unwrap();
            let storage = storages.get(storage_name).context("get storage")?;
            (
                storage.config.schema.clone(),
                storage.config.bucket.clone(),
                path,
            )
        };
        let location = format!("{}://{}/{}", schema, bucket, path);
        println!("export to storage: {}", location);

        match format.to_lowercase().as_str() {
            "csv" => {
                let _ = df.write_csv(&location, Default::default(), None).await?;
            }
            "parquet" => {
                let _ = df
                    .write_parquet(&location, Default::default(), None)
                    .await?;
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
                access_key: env::var("OSS_ACCESS_KEY").unwrap(),
                access_secret: env::var("OSS_ACCESS_SECRET").unwrap(),
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
        db.init_storages(config)?;

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
        db.export_to_storage(result, "oss", "tests/reverse_id.csv", "csv")
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

    // 新增辅助函数
    fn create_test_csv(data_type: &str) -> anyhow::Result<(tempfile::TempDir, std::path::PathBuf)> {
        let mut data = String::from("id,name,amount\n");
        let test_data = match data_type {
            "basic" => {
                r#"id,name,amount
1,Alice,100.50
2,Bob,200.75
3,Charlie,350.25"#
            }
            "empty" => "id,name,amount",
            "large" => {
                for i in 1..1001 {
                    data.push_str(&format!("{},User{},{}.00\n", i, i, i * 100));
                }
                &data
            }
            "special_chars" => {
                r#"id,name,amount
1,"Smith, John",100.50
2,"O'Brien, Mary",200.75
3,"König, Hans",350.25"#
            }
            _ => return Err(anyhow::anyhow!("Unsupported test data type: {}", data_type)),
        };

        let temp_dir = tempdir()?;
        let input_path = temp_dir.path().join("test_data.csv");
        let mut file = File::create(&input_path)?;
        file.write_all(test_data.as_bytes())?;
        println!(
            "Generated temp file for '{}' data, path is: {:?}",
            data_type, input_path
        );

        Ok((temp_dir, input_path))
    }

    #[tokio::test]
    async fn multiple_storage() -> anyhow::Result<()> {
        let (_temp_dir, input_path) = create_test_csv("basic")?;
        println!("input_path: {:?}", input_path);

        let config = Config::from_env().unwrap();
        let db = DB::<()>::new("test_db");
        let res = db.init_storages(config);
        match res {
            Ok(_) => println!("init storages done"),
            Err(e) => println!("init storages failed: {}", e),
        }

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

        db.export_to_storage(result, "minio", "tests/reverse_id.csv", "csv")
            .await?;

        Ok(())
    }
}
