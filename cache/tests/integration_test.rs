use arrow::datatypes::DataType;
use arrow::datatypes::TimeUnit;
use arrow::record_batch::RecordBatch;
use cache::pool::DB;
use std::path::PathBuf;
#[tokio::test]
async fn test_local_csv_operations() -> anyhow::Result<()> {
    let db = DB::<()>::new("test_db");

    // 使用项目中的测试数据文件
    let input_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("data")
        .join("test1.csv");

    // 创建本地表
    let sql = format!(
        r#"
        CREATE EXTERNAL TABLE local_table
        STORED AS CSV
        LOCATION '{}'
       ;"#,
        input_path.to_string_lossy()
    );

    db.query(&sql).await?;

    // 查询并验证数据
    let result = db.query("SELECT * FROM local_table ORDER BY id").await?;
    result.clone().show().await?;
    let batches = result.collect().await?;

    // let json_val = db
    //     .query_to_json("SELECT * FROM local_table ORDER BY id")
    //     .await?;
    // println!("json_val: {}", json_val);

    assert!(!batches.is_empty());
    assert_eq!(batches[0].num_rows(), 3);

    Ok(())
}

#[tokio::test]
/// test_table_csv_type_auto 测试 csv 文件的自动类型推断
async fn test_table_csv_type_auto() -> anyhow::Result<()> {
    // 定义测试用例
    let test_cases = vec![
        (
            "big_decimal.csv",
            vec![
                ("aaid", DataType::Int64),
                ("currency", DataType::Utf8),
                ("account_type", DataType::Utf8),
                ("balance", DataType::Utf8), // 超出 float64 的精度，当成字符串
            ],
        ),
        (
            "cash_changes_check.csv",
            vec![
                ("ledger_date", DataType::Int64), // YYYYMMDD 会被当成 int64
                ("aaid", DataType::Int64),
                ("currency", DataType::Utf8),
                ("created_at", DataType::Timestamp(TimeUnit::Second, None)), // YYYY-MM-DD HH:MM:SS 会被当成 timestamp
                ("biz_code", DataType::Utf8),
                ("ledger_amount", DataType::Float64),
                ("serial_no", DataType::Utf8),
                ("remark", DataType::Utf8),
            ],
        ),
        (
            "test1.csv",
            vec![
                ("id", DataType::Int64),
                ("name", DataType::Utf8),
                ("amount", DataType::Float64), // 一般的数据会当成  float64 类型
            ],
        ),
        ("decimal_first.csv", vec![("cash", DataType::Float64)]),
    ];

    for (file_name, expected_types) in test_cases {
        println!("Testing file: {}", file_name);

        // 构建文件路径
        let input_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("data")
            .join(file_name);

        // 创建数据库实例
        let db = DB::<()>::new("test_db");

        // 创建表
        let sql = format!(
            r#"
            CREATE EXTERNAL TABLE test_table 
            STORED AS CSV
            LOCATION '{}'
            ;"#,
            input_path.to_string_lossy()
        );

        db.query(&sql).await?;

        // 查询并验证类型
        let result = db.query("SELECT * FROM test_table LIMIT 1").await?;
        let actual_schema = result.schema();
        // 验证每个字段的类型
        for (name, expected_type) in expected_types {
            let field = actual_schema.field_with_name(None, name)?;
            println!(
                "test file {} field: {:?} type is {}",
                file_name,
                field,
                field.data_type()
            );
            assert_eq!(
                field.data_type(),
                &expected_type,
                "Field '{}' in {} has wrong type",
                name,
                file_name
            );
        }
        println!("--------------------------------");
    }

    Ok(())
}

#[tokio::test]
/// test_table_csv_type_concret 测试 csv 文件的设置类型
async fn test_table_csv_type_concret() -> anyhow::Result<()> {
    // 定义测试用例
    let test_cases = vec![
        // (
        //     "big_decimal.csv",
        //     "create table test_table (aaid int64, currency string, account_type string, balance string) stored as csv location 'tests/data/big_decimal.csv';",
        //     vec![
        //         ("aaid", DataType::Int64),
        //         ("currency", DataType::Utf8),
        //         ("account_type", DataType::Utf8),
        //         ("balance", DataType::Utf8), // 超出 float64 的精度，当成字符串
        //     ],
        // ),
        (
            "cash_changes_check.csv",
            r#"CREATE EXTERNAL TABLE test_table 
            (ledger_date date, aaid bigint, currency string, created_at timestamp, biz_code string, 
            ledger_amount decimal, serial_no string, remark string) 
            STORED AS CSV
            location 'tests/data/cash_changes_check.csv';"#,
            vec![
                ("ledger_date", DataType::Date32), // date -> Date32
                ("aaid", DataType::Int64),
                ("currency", DataType::Utf8),
                (
                    "created_at",
                    DataType::Timestamp(TimeUnit::Nanosecond, None), // default is nanosecond
                ),
                ("biz_code", DataType::Utf8),
                ("ledger_amount", DataType::Decimal128(38, 10)), // decimal -> Decimal128(38, 10)
                ("serial_no", DataType::Utf8),
                ("remark", DataType::Utf8),
            ],
        ),
        // (
        //     "test1.csv",
        //     r#"create table test_table
        //     (id int64, name string, amount float64)
        //     STORED AS CSV
        //     location 'tests/data/test1.csv';"#,
        //     vec![
        //         ("id", DataType::Int64),
        //         ("name", DataType::Utf8),
        //         ("amount", DataType::Float64), // 一般的数据会当成  float64 类型
        //     ],
        // ),
        // (
        //     "decimal_first.csv",
        //     r#"create table test_table
        //     (cash float64)
        //     STORED AS CSV
        //     location 'tests/data/decimal_first.csv';"#,
        //     vec![("cash", DataType::Float64)],
        // ),
    ];

    for (file_name, create_table_sql, expected_types) in test_cases {
        println!("Testing file: {}", file_name);

        // 构建文件路径
        let input_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("data")
            .join(file_name);

        // 创建数据库实例
        let db = DB::<()>::new("test_db");
        db.query(&create_table_sql).await?;

        // 查询并验证类型
        let result = db.query("SELECT * FROM test_table LIMIT 1").await?;
        let actual_schema = result.schema();
        // 验证每个字段的类型
        for (name, expected_type) in expected_types {
            let field = actual_schema.field_with_name(None, name)?;
            println!(
                "test file {} field: {:?} type is {}",
                file_name,
                field,
                field.data_type()
            );
            assert_eq!(
                field.data_type(),
                &expected_type,
                "Field '{}' in {} has wrong type",
                name,
                file_name
            );
        }
        println!("--------------------------------");
    }

    Ok(())
}
