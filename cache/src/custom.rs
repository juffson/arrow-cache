use std::any::Any;
use std::collections::BTreeMap; // BTreeMap 可以保持分区有序

use arrow::array::StringArray;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::{
    catalog::{Session, TableProviderFactory},
    datasource::{TableProvider, TableType, memory::MemorySourceConfig},
    error::DataFusionError,
    execution::{SendableRecordBatchStream, TaskContext},
    logical_expr::{CreateExternalTable, Expr, dml::InsertOp},
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan,
        insert::{DataSink, DataSinkExec},
    },
};
use futures::StreamExt;
use std::sync::{Arc, Mutex, RwLock}; // 需要线程安全访问

// 定义分区键的类型，这里用 String 举例，可以是日期、类别等
type PartitionKey = String;

// 自定义的 TableProvider
#[derive(Debug, Clone)]
pub struct PartitionedMemTable {
    schema: SchemaRef,
    // 使用 RwLock 允许多读单写
    // BTreeMap 的 Key 是分区标识，Value 是该分区对应的 RecordBatch 列表
    partitions: Arc<RwLock<BTreeMap<PartitionKey, Vec<RecordBatch>>>>,
    pub partition_key: String,
}

#[derive(Clone)]
pub struct PartitionedMemTableSink {
    m: PartitionedMemTable,
}

impl PartitionedMemTableSink {
    fn new(m: PartitionedMemTable) -> Self {
        Self { m }
    }
}

impl std::fmt::Debug for PartitionedMemTableSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "partitioned mem table sink")
    }
}

impl DisplayAs for PartitionedMemTableSink {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "partitioned mem table sink")
    }
}

impl PartitionedMemTable {
    pub fn new(schema: SchemaRef, partition_key: String) -> Self {
        Self {
            schema,
            partitions: Arc::new(RwLock::new(BTreeMap::new())),
            partition_key,
        }
    }

    // --- 核心方法：插入数据到指定分区 ---
    pub fn insert_partition(&self, key: PartitionKey, batch: RecordBatch) -> anyhow::Result<()> {
        // 校验 Batch Schema 是否匹配
        if batch.schema() != self.schema {
            return Err(anyhow::anyhow!(
                "RecordBatch schema does not match table schema"
            ));
        }

        let mut partitions_guard = self.partitions.write().unwrap(); // 获取写锁
        partitions_guard.entry(key).or_default().push(batch);
        Ok(())
    }

    // --- 核心方法：删除指定分区 ---
    pub fn drop_partition(
        &self,
        key: &PartitionKey,
    ) -> Result<Option<Vec<RecordBatch>>, anyhow::Error> {
        let mut partitions_guard = self.partitions.write().unwrap(); // 获取写锁
        Ok(partitions_guard.remove(key))
    }

    // --- 辅助方法：获取所有分区键 ---
    pub fn get_partition_keys(&self) -> Vec<PartitionKey> {
        let partitions_guard = self.partitions.read().unwrap(); // 获取读锁
        partitions_guard.keys().cloned().collect()
    }
}

// --- 实现 TableProvider Trait ---
#[async_trait::async_trait]
impl TableProvider for PartitionedMemTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base // 表示这是一个基础表
    }

    // --- 核心方法：创建物理执行计划 (Scan) ---
    async fn scan(
        &self,
        _state: &dyn Session,            // SessionState 可能包含查询相关的上下文
        projection: Option<&Vec<usize>>, // 需要读取的列索引
        _filters: &[Expr],               // 下推的过滤器表达式 (重要!)
        _limit: Option<usize>,           // 下推的 limit
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let partitions_guard = self.partitions.read().unwrap(); // 获取读锁
        let mut batches_to_scan = Vec::new();
        for (_key, batches) in partitions_guard.iter() {
            // 注意：这里直接 extend 可能导致大量小 batch，实际中可能需要合并
            batches_to_scan.extend(batches.clone());
        }

        // 如果没有数据，返回空的执行计划
        if batches_to_scan.is_empty() {
            return Ok(MemorySourceConfig::try_new_exec(
                &[],
                self.schema(),
                projection.cloned(),
            )?);
        }

        // 使用 MemoryExec 来执行内存数据的扫描
        // MemoryExec 需要一个 Vec<Vec<RecordBatch>>，外层 Vec 代表物理分区
        // 在这个简单例子里，我们只有一个物理分区包含所有数据
        let exec = MemorySourceConfig::try_new_exec(
            &[batches_to_scan],
            self.schema(),
            projection.cloned(),
        )?;
        Ok(exec)
    }

    // --- Implementation for INSERT INTO ---
    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>, // This plan represents the data from VALUES/SELECT
        insert_op: InsertOp,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(DataSinkExec::new(
            input,
            Arc::new(PartitionedMemTableSink::new(self.clone())),
            None,
        )) as _)
    }
}

#[derive(Debug, Default)] // Default is handy for simple factories
pub struct PartitionedMemTableFactory {}

#[async_trait::async_trait]
impl TableProviderFactory for PartitionedMemTableFactory {
    async fn create(
        &self,
        _state: &dyn Session,
        cmd: &CreateExternalTable,
    ) -> datafusion::error::Result<Arc<dyn TableProvider>> {
        // 1. Extract the schema from the DDL command
        // DataFusion parses the schema defined in "CREATE EXTERNAL TABLE my_table (col1 INT, ...)"
        let schema = Arc::new(cmd.schema.as_ref().clone());
        let s = schema.as_arrow();
        println!("function create schema: {:?}", s);

        // 假设一定有 partition
        let partition: Vec<String> = cmd.table_partition_cols.clone();
        let partition_key = partition.first().unwrap().to_string();
        println!("function create partition_key: {:?}", partition_key);

        // 2. Optionally use LOCATION or other parameters if needed
        // For this simple case, we might not need LOCATION if the table name itself is sufficient
        // let location = &cmd.location;
        // You could potentially parse configuration from the location URL
        // e.g., location = "partitioned_mem://my_table?ttl=3600"

        // 3. Create your custom TableProvider instance
        let table_provider = Arc::new(PartitionedMemTable::new(Arc::new(s.clone()), partition_key));

        // 4. Optional: Start the background cleanup task if you implemented it
        // You might need access to the Tokio runtime handle or pass config via SessionState
        // PartitionedMemTable::start_cleanup_task(table_provider.clone(), Duration::from_secs(60)); // Example

        // 5. Return the provider
        Ok(table_provider)
    }
}

#[async_trait::async_trait]
impl DataSink for PartitionedMemTableSink {
    fn schema(&self) -> &SchemaRef {
        return &self.m.schema;
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn metrics(&self) -> Option<datafusion::physical_plan::metrics::MetricsSet> {
        None
    } // Or implement metrics

    // This method consumes the stream produced by the input plan
    async fn write_all(
        &self,
        mut stream: SendableRecordBatchStream,
        _context: &Arc<TaskContext>, // Context is available if needed
    ) -> datafusion::error::Result<u64> {
        let mut rows_inserted: u64 = 0;

        while let Some(batch_result) = stream.next().await {
            let batch = batch_result?;
            println!("write all: {:?}", batch.num_rows());
            if batch.num_rows() == 0 {
                continue;
            }

            // 一批数据的 partition 肯定是一样的，不允许写不同的 partition
            let partition_array = batch
                .column_by_name(self.m.partition_key.as_str())
                .unwrap()
                .clone();

            let partition_key: PartitionKey = if let Some(string_array) =
                partition_array.as_any().downcast_ref::<StringArray>()
            {
                string_array.value(0).to_string() // .value() is efficient for StringArray
            } else {
                return Err(DataFusionError::Execution(format!(
                    "Partition key column '{}' in batch is not Utf8 (String)",
                    self.m.partition_key
                )));
            };
            println!("batch is: {:?}", batch.clone());

            let partition_key = "tool".to_string();
            let num_rows = batch.num_rows() as u64;
            // Logic to insert batch into the target table
            self.m.insert_partition(partition_key, batch).unwrap();
            println!("inserted data: {:?}", self.m);
            rows_inserted += num_rows;
        }
        Ok(rows_inserted) // Return the number of rows inserted
    }
}

mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray, TimestampNanosecondArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use arrow::record_batch::RecordBatch;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::execution::context::SessionContext;
    use datafusion::execution::runtime_env::RuntimeEnv;
    use datafusion::functions_array::length;

    use std::sync::Arc;
    #[tokio::test]
    async fn test_partitioned_mem_table() -> anyhow::Result<()> {
        let ctx = SessionContext::new();

        // 1. 定义 Schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            // 可以加一个显式的分区列，虽然我们用外部 key 管理
            Field::new("partition_col", DataType::Utf8, false),
        ]));

        // 2. 创建自定义的 TableProvider 实例
        let table_provider = Arc::new(PartitionedMemTable::new(schema.clone(), "dt".to_string()));

        // 3. 注册 TableProvider 到 DataFusion
        ctx.register_table("my_partitioned_table", table_provider.clone())?;

        // 4. 插入数据到不同分区
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(TimestampNanosecondArray::from(vec![
                    1704067200000000000,
                    1704067260000000000,
                ])), // 2024-01-01 ...
                Arc::new(arrow::array::StringArray::from(vec![
                    "2024-01-01",
                    "2024-01-01",
                ])),
            ],
        )?;
        table_provider.insert_partition("2024-01-01".to_string(), batch1)?;

        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![3, 4])),
                Arc::new(TimestampNanosecondArray::from(vec![
                    1704153600000000000,
                    1704153660000000000,
                ])), // 2024-01-02 ...
                Arc::new(arrow::array::StringArray::from(vec![
                    "2024-01-02",
                    "2024-01-02",
                ])),
            ],
        )?;
        table_provider.insert_partition("2024-01-02".to_string(), batch2)?;

        println!(
            "Partitions before drop: {:?}",
            table_provider.get_partition_keys()
        );

        // 5. 查询数据 (会扫描所有分区，除非实现分区裁剪)
        let df = ctx.sql("SELECT count(*) FROM my_partitioned_table").await?;
        df.show().await?;

        // 6. 删除一个分区 (通过 Rust 代码直接调用)
        println!("Dropping partition '2024-01-01'...");
        if let Some(dropped_batches) = table_provider.drop_partition(&"2024-01-01".to_string())? {
            println!(
                "Dropped {} batches for partition '2024-01-01'",
                dropped_batches.len()
            );
        } else {
            println!("Partition '2024-01-01' not found.");
        }

        println!(
            "Partitions after drop: {:?}",
            table_provider.get_partition_keys()
        );

        // 7. 再次查询，数据应该减少了
        let df_after_drop = ctx.sql("SELECT count(*) FROM my_partitioned_table").await?;
        df_after_drop.show().await?;

        // 你可以继续插入新的分区数据...
        let batch3 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![5])),
                Arc::new(TimestampNanosecondArray::from(vec![1704240000000000000])), // 2024-01-03 ...
                Arc::new(arrow::array::StringArray::from(vec!["2024-01-03"])),
            ],
        )?;
        table_provider.insert_partition("2024-01-03".to_string(), batch3)?;
        println!(
            "Inserted partition '2024-01-03'. Current partitions: {:?}",
            table_provider.get_partition_keys()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_create_external_table() -> anyhow::Result<()> {
        // 1. 创建 SessionContext 并注册 Factory
        // 使用 "partitioned_mem" 作为 scheme，当 LOCATION 以 "partitioned_mem://" 开头时触发
        let mut config = datafusion::execution::context::SessionConfig::new();
        let mut builder = SessionStateBuilder::new()
            .with_default_features()
            .with_config(config);

        let table_factor = PartitionedMemTableFactory::default();
        builder = builder.with_table_factory("MEM".to_string(), Arc::new(table_factor));

        let state = builder.build();
        let ctx = SessionContext::new_with_state(state);

        // 2. 执行 CREATE EXTERNAL TABLE SQL
        let create_sql = r#"
            CREATE EXTERNAL TABLE my_dynamic_table (
                id INT,
                value STRING,
                part STRING
            )
            STORED AS mem
            PARTITIONED BY (part)
            LOCATION 'mem://data'
        "#;
        ctx.sql(create_sql).await?.collect().await?;

        // 3. 验证表已创建 (尝试查询)
        // 这会失败，因为还没有数据，但如果表不存在，ctx.sql 会返回 TableNotFound 错误
        let df_empty = ctx.sql("SELECT count(*) FROM my_dynamic_table").await?;
        let results_empty = df_empty.collect().await?;
        assert_eq!(results_empty.len(), 1);
        // 你可以进一步检查结果是否为 count = 0

        println!("Table 'my_dynamic_table' created successfully via SQL.");

        let res = ctx
            .sql(r#"INSERT INTO my_dynamic_table VALUES (1, 'bbbb', 'tool')"#)
            .await?
            .collect()
            .await?;
        println!("inserted data: {:?}", res.len());
        let df_data = ctx
            .sql("SELECT id, value, part FROM my_dynamic_table")
            .await?;
        df_data.show().await?; // Should show count = 3

        // 4. (可选) 获取 TableProvider 实例并插入数据
        let table_ref = ctx.table_provider("my_dynamic_table").await?;
        let mem_table = table_ref
            .as_any()
            .downcast_ref::<PartitionedMemTable>()
            .expect("TableProvider should be PartitionedMemTable");

        let schema = mem_table.schema(); // Get schema from the created table
        let batch_dyn = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![101, 102])),
                Arc::new(StringArray::from(vec!["apple", "banana"])),
                Arc::new(StringArray::from(vec!["fruit", "fruit"])),
            ],
        )?;
        mem_table.insert_partition("fruit".to_string(), batch_dyn)?;

        let batch_dyn2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![201])),
                Arc::new(StringArray::from(vec!["carrot"])),
                Arc::new(StringArray::from(vec!["vegetable"])),
            ],
        )?;
        mem_table.insert_partition("vegetable".to_string(), batch_dyn2)?;

        println!(
            "Inserted data. Current partitions: {:?}",
            mem_table.get_partition_keys()
        );

        println!("get_partition_keys: {:?}", mem_table.get_partition_keys());

        // 5. 查询验证数据
        let df_data = ctx.sql("SELECT count(*) FROM my_dynamic_table").await?;
        df_data.show().await?; // Should show count = 3

        let df_filter = ctx
            .sql("SELECT id, value FROM my_dynamic_table WHERE part = 'fruit'")
            .await?;
        df_filter.show().await?; // Should show apple and banana rows

        Ok(())
    }
}
