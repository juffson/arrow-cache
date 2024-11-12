use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::DataFusionError;
use datafusion::config::ConfigOptions;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_expr::PhysicalSortRequirement;
use datafusion::physical_plan::ExecutionMode;
use datafusion::physical_plan::Partitioning;
use datafusion::physical_plan::{
    DisplayAs, Distribution, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use std::any::Any;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct ClickHouseTableProvider {
    // ClickHouse 连接信息等
}
impl ClickHouseTableProvider {
    pub fn new() -> Self {
        Self {}
    }

    pub async fn create_physical_plan(&self, schema: SchemaRef) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(ClickHouseExecutionPlan::new(schema, self.clone())))
    }
}
#[async_trait]
impl TableProvider for ClickHouseTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        return self.create_physical_plan(self.schema()).await;
    }

    // TODO 通过 cache pool 统一 schema
    fn schema(&self) -> SchemaRef {
        // 创建字段列表
        let fields = vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int32, true),
            Field::new("email", DataType::Utf8, true),
            // 添加更多字段...
        ];

        // 创建 Schema
        let schema = Schema::new(fields);

        // 将 Schema 包装在 Arc 中并返回
        Arc::new(schema)
    }
}
#[derive(Debug)]
struct ClickHouseExecutionPlan {
    schema: SchemaRef,
    properties: PlanProperties,
    db: ClickHouseTableProvider,
}

impl ClickHouseExecutionPlan {
    fn new(schema: SchemaRef, db: ClickHouseTableProvider) -> Self {
        // 创建 EquivalenceProperties
        let eq_properties = EquivalenceProperties::new(schema.clone());

        // 设置 Partitioning，这里假设 ClickHouse 查询结果是单个分区
        let partitioning = Partitioning::UnknownPartitioning(1);

        // 设置 ExecutionMode，这里假设是 ExecutionMode::Exec
        let execution_mode = ExecutionMode::Unbounded;

        // 创建 PlanProperties
        let properties = PlanProperties::new(eq_properties, partitioning, execution_mode);

        Self {
            schema,
            properties,
            db,
        }
    }
}

impl ExecutionPlan for ClickHouseExecutionPlan {
    fn name(&self) -> &str {
        "ClickHouseExecutionPlan"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![] // ClickHouse 执行计划没有子计划
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // 实现查询执行逻辑
        todo!("Implement query execution")
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![] // 没有子计划，所以返回空向量
    }

    fn required_input_ordering(&self) -> Vec<Option<datafusion::physical_expr::LexRequirement>> {
        vec![] // 没有子计划，所以返回空向量
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![] // 没有子计划，所以返回空向量
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![] // 没有子计划，所以返回空向量
    }

    fn repartitioned(
        &self,
        _target_partitions: usize,
        _config: &ConfigOptions,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        // ClickHouse 执行计划可能不支持重分区
        Ok(None)
    }
}

impl DisplayAs for ClickHouseExecutionPlan {
    fn fmt_as(
        &self,
        t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            datafusion::physical_plan::DisplayFormatType::Default => {
                write!(f, "ClickHouseExecutionPlan")
            }
            datafusion::physical_plan::DisplayFormatType::Verbose => {
                writeln!(f, "ClickHouseExecutionPlan:")?;
                writeln!(f, "  Schema: {:?}", self.schema)?;
                writeln!(f, "  Partitioning: {:?}", self.properties.partitioning)?;
                writeln!(
                    f,
                    "  Execution Mode: {:?}",
                    self.properties.execution_mode()
                )?;
                writeln!(
                    f,
                    "  Output Ordering: {:?}",
                    self.properties.output_ordering()
                )
            }
        }
    }
}
