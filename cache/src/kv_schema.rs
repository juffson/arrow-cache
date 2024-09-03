use crate::ck::ClickHouseTableProvider;
use anyhow::{Ok, Result};
use datafusion::arrow::array::{ArrayRef, BooleanArray, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::TableProvider;
use datafusion::prelude::*;
use serde::{de::DeserializeOwned, Serialize};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;

pub struct KVSchema {
    sch: SchemaRef,
}
