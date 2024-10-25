use crate::ck::ClickHouseTableProvider;
use anyhow::{Ok, Result};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, StringArray, UInt64Array,
};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::TableProvider;
use datafusion::prelude::*;
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;
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
    pub async fn create_table(&self, s: SchemaRef) -> Result<()> {
        let empty_batch = RecordBatch::try_new(s.clone(), create_empty_columns(&s))?;

        let context = self.ctx.write().await;
        context.register_batch(&self.id, empty_batch)?;
        Ok(())
    }

    pub async fn create_table_with_provider(&self, s: SchemaRef) -> Result<()> {
        let empty_batch = RecordBatch::try_new(s.clone(), create_empty_columns(&s))?;
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
        let df = context
            .sql(sql)
            .await
            .map_err(|e| anyhow::anyhow!("Query error: {}", e));
        df
    }

    pub async fn query_to_schema(&self, sql: &str) -> Result<Vec<V>> {
        let batches = self.query_to_batches(sql).await?;
        let mut results = Vec::new();

        for batch in batches {
            let schema = batch.schema();
            let num_rows = batch.num_rows();

            for row_index in 0..num_rows {
                let mut row_obj = serde_json::Map::new();

                for (col_index, field) in schema.fields().iter().enumerate() {
                    let column = batch.column(col_index);
                    let value = get_value_at(column, row_index)?;
                    row_obj.insert(field.name().clone(), value);
                }

                let row_value = Value::Object(row_obj);
                let row_struct: V = serde_json::from_value(row_value)?;
                results.push(row_struct);
            }
        }
        Ok(results)
    }

    pub async fn query_to_json(&self, sql: &str) -> anyhow::Result<serde_json::Value> {
        let batches = self.query_to_batches(sql).await?;
        for batch in batches {
            let schema = batch.schema();
            let num_rows = batch.num_rows();

            for row_index in 0..num_rows {
                let mut row_obj = serde_json::Map::new();

                for (col_index, field) in schema.fields().iter().enumerate() {
                    let column = batch.column(col_index);
                    let value = get_value_at(column, row_index)?;
                    row_obj.insert(field.name().clone(), value);
                }

                let row_value = Value::Object(row_obj);
                let row_struct: V = serde_json::from_value(row_value)?;

                return Ok(serde_json::to_value(row_struct)?);
            }
        }
        Ok(serde_json::Value::Null)
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

    pub async fn truncate(&self) -> Result<()> {
        //let c = self.ctx.write().await;
        // TODO support truncate
        Ok(())
    }

    pub async fn recovery(&self) -> Result<()> {
        // TODO recovery from clickhouse/wal
        Ok(())
    }
}

fn create_empty_columns(schema: &SchemaRef) -> Vec<ArrayRef> {
    schema
        .fields()
        .iter()
        .map(|field| match field.data_type() {
            DataType::Boolean => {
                Arc::new(BooleanArray::from(Vec::<Option<bool>>::new())) as ArrayRef
            }
            DataType::Int32 => Arc::new(Int32Array::from(Vec::<Option<i32>>::new())) as ArrayRef,
            DataType::Int64 => Arc::new(Int64Array::from(Vec::<Option<i64>>::new())) as ArrayRef,
            DataType::UInt64 => Arc::new(UInt64Array::from(Vec::<Option<u64>>::new())) as ArrayRef,
            DataType::Float64 => {
                Arc::new(Float64Array::from(Vec::<Option<f64>>::new())) as ArrayRef
            }

            DataType::Utf8 => Arc::new(StringArray::from(Vec::<Option<&str>>::new())) as ArrayRef,
            // 可以根据需要添加更多数据类型的处理
            _ => panic!("Unsupported data type: {:?}", field.data_type()),
        })
        .collect()
}

fn get_value_at(column: &ArrayRef, index: usize) -> Result<Value> {
    Ok(match column.data_type() {
        DataType::Boolean => Value::Bool(
            column
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(index),
        ),
        DataType::Int32 => Value::Number(
            column
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(index)
                .into(),
        ),
        DataType::Int64 => Value::Number(
            column
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(index)
                .into(),
        ),
        DataType::UInt64 => Value::Number(
            column
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(index)
                .into(),
        ),
        DataType::Float64 => {
            let float_val = column
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(index);
            serde_json::Number::from_f64(float_val)
                .map(Value::Number)
                .unwrap_or(Value::Null)
        }
        DataType::Utf8 => Value::String(
            column
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(index)
                .to_string(),
        ),
        _ => {
            return Err(anyhow::anyhow!(
                "Unsupported data type: {:?}",
                column.data_type()
            ))
        }
    })
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
        db.create_table(schema).await?;

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

        db.create_table(schema).await?;

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

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct TestUser {
        id: i64,
        name: String,
        age: i32,
    }

    #[tokio::test]
    async fn test_query_to_schema() -> Result<()> {
        // 创建一个新的 DB 实例
        let db = DB::<TestUser>::new("test_db");

        // 创建一个临时表
        db.execute("CREATE TABLE test_users (id BIGINT, name VARCHAR, age INT)")
            .await?;

        // 插入测试数据
        db.execute("INSERT INTO test_users (id, name, age) VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35)")
        .await?;

        // 使用 query_to_schema 方法查询数据
        let results: Vec<TestUser> = db
            .query_to_schema("SELECT * FROM test_users ORDER BY id")
            .await?;

        // 验证结果
        let expected = vec![
            TestUser {
                id: 1,
                name: "Alice".to_string(),
                age: 30,
            },
            TestUser {
                id: 2,
                name: "Bob".to_string(),
                age: 25,
            },
            TestUser {
                id: 3,
                name: "Charlie".to_string(),
                age: 35,
            },
        ];

        assert_eq!(
            results, expected,
            "Query results do not match expected data"
        );

        // 测试部分查询
        let partial_results: Vec<TestUser> = db
            .query_to_schema("SELECT id, name, age FROM test_users WHERE age > 25 ORDER BY id")
            .await?;

        let expected_partial = vec![
            TestUser {
                id: 1,
                name: "Alice".to_string(),
                age: 30,
            },
            TestUser {
                id: 3,
                name: "Charlie".to_string(),
                age: 35,
            },
        ];

        assert_eq!(
            partial_results, expected_partial,
            "Partial query results do not match expected data"
        );

        // 清理：删除测试表
        db.execute("DROP TABLE test_users").await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_create_table_with_provider() -> Result<()> {
        let db = DB::<TestUser>::new("test_db");
        // Create table
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("timestamp", DataType::Int64, false),
            Field::new("is_deleted", DataType::Boolean, false),
        ]));
        db.create_table_with_provider(schema).await.unwrap();
        Ok(())
    }
}
