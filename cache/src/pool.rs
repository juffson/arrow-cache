use crate::ck::ClickHouseTableProvider;
use anyhow::{Ok, Result};
use arrow::datatypes::SchemaRef;
use datafusion::arrow::array::{ArrayRef, BooleanArray, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::TableProvider;
use datafusion::prelude::*;
use serde::{de::DeserializeOwned, Serialize};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;

const DEFAULT_SYNC_INTERVAL: Duration = Duration::from_secs(30);
pub struct DB<V: Serialize + DeserializeOwned + Send + Sync> {
    pub id: String,
    ctx: Arc<RwLock<SessionContext>>,
    _phantom: std::marker::PhantomData<V>,
    sync_interval: Duration,
}

impl<V: Serialize + DeserializeOwned + Send + Sync> DB<V> {
    pub fn new(id: &str) -> Self {
        Self {
            id: id.to_string(),
            ctx: Arc::new(RwLock::new(SessionContext::new())),
            _phantom: std::marker::PhantomData,
            sync_interval: DEFAULT_SYNC_INTERVAL,
        }
    }

    // create table
    // use arrow schema & arrow array to create table
    pub async fn create_table(&self, s: SchemaRef, cols: Vec<ArrayRef>) -> Result<()> {
        let empty_batch = RecordBatch::try_new(s.clone(), cols)?;

        let context = self.ctx.write().await;
        context.register_batch(&self.id, empty_batch)?;
        Ok(())
    }

    pub async fn create_table_with_provider(
        &self,
        s: SchemaRef,
        cols: Vec<ArrayRef>,
    ) -> Result<()> {
        let empty_batch = RecordBatch::try_new(s.clone(), cols)?;
        let context = self.ctx.write().await;
        context.register_batch(&self.id, empty_batch)?;
        // read from source
        // TODO support clickhouse
        let provider = Arc::new(ClickHouseTableProvider::new()) as Arc<dyn TableProvider>;
        // not sync data
        let _ = context.read_table(provider)?;
        Ok(())
    }

    pub async fn query(&self, sql: &str) -> Result<DataFrame> {
        let context = self.ctx.read().await;
        context
            .sql(sql)
            .await
            .map_err(|e| anyhow::anyhow!("Query error: {}", e))
    }

    pub async fn query_to_batches(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        let df = self.query(sql).await?;
        df.collect()
            .await
            .map_err(|e| anyhow::anyhow!("Error collecting results: {}", e))
    }

    pub async fn insert(&self, sql: &str) -> Result<()> {
        self.execute(sql).await
    }

    pub async fn execute(&self, sql: &str) -> Result<()> {
        let context = self.ctx.write().await;
        context.sql(sql).await?.collect().await?;
        Ok(())
    }

    pub async fn recovery(&self) -> Result<()> {
        // TODO recovery from clickhouse/wal
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
    struct CustomValue {
        #[serde(default)]
        field1: String,
        #[serde(default)]
        field2: i32,
    }
    impl Default for CustomValue {
        fn default() -> Self {
            CustomValue {
                field1: String::new(),
                field2: 0,
            }
        }
    }

    #[tokio::test]
    async fn test_create_and_insert() -> Result<()> {
        let db = DB::<CustomValue>::new("test_table");

        // Create table
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, false),
            Field::new("timestamp", DataType::Int64, false),
            Field::new("is_deleted", DataType::Boolean, false),
        ]));

        let cols: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec![""])) as ArrayRef,
            Arc::new(StringArray::from(vec![""])) as ArrayRef,
            Arc::new(Int64Array::from(vec![0])) as ArrayRef,
            Arc::new(BooleanArray::from(vec![false])) as ArrayRef,
        ];

        db.create_table(schema, cols).await?;

        // Insert data
        db.insert("INSERT INTO test_table VALUES ('key1', 'value1', 1234567890, false)")
            .await?;

        // Query data
        let _ = db.execute("SELECT * FROM test_table").await?;

        // Add assertions here to check the result

        Ok(())
    }

    #[tokio::test]
    async fn test_query() -> Result<()> {
        let db = DB::<CustomValue>::new("test_table");

        // 创建表并插入一些数据

        // Create table
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, false),
            Field::new("timestamp", DataType::Int64, false),
            Field::new("is_deleted", DataType::Boolean, false),
        ]));

        let cols: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec![""])) as ArrayRef,
            Arc::new(StringArray::from(vec![""])) as ArrayRef,
            Arc::new(Int64Array::from(vec![0])) as ArrayRef,
            Arc::new(BooleanArray::from(vec![false])) as ArrayRef,
        ];

        db.create_table(schema, cols).await?;

        // Insert data
        db.insert("INSERT INTO test_table VALUES ('key1', 'value1', 1234567890, false)")
            .await?;

        let _df = db
            .query("SELECT * FROM test_table WHERE key = 'key1'")
            .await?;

        // 使用 query_to_batches 方法
        let batches = db.query_to_batches("SELECT * FROM test_table").await?;
        for batch in batches {
            println!("Batch: {:?}", batch);
        }

        Ok(())
    }
}
