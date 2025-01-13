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
