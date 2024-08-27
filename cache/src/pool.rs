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
        //let df = context.read_table(provider)?;
        Ok(())
    }

    pub async fn insert(&self, sql: &str) -> Result<()> {
        self.execute(sql).await
    }
    //pub async fn query(&self, sql: &str) -> Result<DataFrame> {
    //    let context = self.ctx.read().await;
    //    Ok(())
    //}
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
    use std::time::Duration;

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
}
