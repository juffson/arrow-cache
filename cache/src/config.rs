use config::{Config as ConfigRs, ConfigError, Environment, File};
use serde::Deserialize;
use std::collections::HashMap;

#[derive(Debug, Deserialize)]
pub struct StorageConfig {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub endpoint: Option<String>,
    pub region: String,
    pub bucket: String,
}

#[derive(Debug, Deserialize)]
pub struct Config {
    pub storages: HashMap<String, StorageConfig>,
}

impl Config {
    pub fn load() -> Result<Self, ConfigError> {
        let settings = ConfigRs::builder()
            .add_source(File::with_name("config/default"))
            .add_source(File::with_name("config/local").required(false))
            .add_source(Environment::with_prefix("APP").separator("__"))
            .build()?;

        settings.try_deserialize()
    }

    pub fn from_env() -> Result<Self, ConfigError> {
        let settings = ConfigRs::builder()
            .add_source(Environment::with_prefix("APP").separator("__"))
            .build()?;

        settings.try_deserialize()
    }

    pub fn from_file(path: &str) -> Result<Self, ConfigError> {
        let settings = ConfigRs::builder()
            .add_source(File::with_name(path))
            .build()?;

        settings.try_deserialize()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn test_from_file() {
        let config = Config::from_file("tests/config/test").unwrap();
        assert_eq!(config.storages.len(), 2);

        let s3_config = config.storages.get("s3").unwrap();
        assert_eq!(s3_config.bucket, "test-bucket");
        assert_eq!(s3_config.region, "us-east-1");

        let minio_config = config.storages.get("minio").unwrap();
        assert_eq!(minio_config.bucket, "test-minio-bucket");
        assert!(minio_config.endpoint.is_some());
    }

    #[test]
    fn test_from_env() {
        // 设置测试环境变量
        env::set_var("APP__STORAGES__s3__ACCESS_KEY_ID", "test_key");
        env::set_var("APP__STORAGES__s3__SECRET_ACCESS_KEY", "test_secret");
        env::set_var("APP__STORAGES__s3__REGION", "us-east-1");
        env::set_var("APP__STORAGES__s3__BUCKET", "test-bucket");

        let config = Config::from_env().unwrap();
        assert_eq!(config.storages.len(), 1);

        let s3_config = config.storages.get("s3").unwrap();
        assert_eq!(s3_config.access_key_id, "test_key");
        assert_eq!(s3_config.bucket, "test-bucket");

        // 清理环境变量
        env::remove_var("APP__STORAGES__s3__ACCESS_KEY_ID");
        env::remove_var("APP__STORAGES__s3__SECRET_ACCESS_KEY");
        env::remove_var("APP__STORAGES__s3__REGION");
        env::remove_var("APP__STORAGES__s3__BUCKET");
    }

    #[test]
    fn test_load_with_override() {
        // 设置环境变量来覆盖文件配置
        env::set_var("APP__STORAGES__s3__ACCESS_KEY_ID", "override_key");

        let config = Config::load().unwrap();
        let s3_config = config.storages.get("s3").unwrap();
        assert_eq!(s3_config.access_key_id, "override_key");

        env::remove_var("APP__STORAGES__s3__ACCESS_KEY_ID");
    }
}
