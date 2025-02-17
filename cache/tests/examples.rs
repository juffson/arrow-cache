use anyhow::Context;
use cache::config::Config;
use cache::pool::DB;
use object_store::path::Path;
use object_store::PutPayload;
use std::env;
use std::path::PathBuf;

// 设置环境变量
// APP__STORAGES__minio__ACCESS_KEY=ak
// APP__STORAGES__minio__ACCESS_SECRET=sk
// APP__STORAGES__minio__ENDPOINT=https://localhost:9000
// APP__STORAGES__minio__REGION=us-east-1
// APP__STORAGES__minio__BUCKET=demo
// APP__STORAGES__minio__SCHEMA=minio

#[tokio::test]
/// Example 1: Read-only operations with external table
/// 1. Create external table from remote/local csv
/// 2. Query data (read-only)
/// 3. Export results to remote/local csv
async fn example_readonly_operations() -> anyhow::Result<()> {
    let db = DB::<()>::new("example_db");
    let config = Config::from_env().context("get config error, you should set env")?;
    db.init_storages(config)?;
    // Get test file path
    let file_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("data")
        .join("test1.csv");
    let file_content = tokio::fs::read(&file_path).await?;
    let input_path = Path::from_url_path("demo/test1.csv").unwrap();
    let store = {
        let storages = db.registered_storages.read().unwrap();
        let storage = storages.get("minio").context("get storage")?;
        storage.store.clone()
    };

    let path = store
        .put(
            (&input_path).into(),
            PutPayload::from_bytes(file_content.into()),
        )
        .await?;
    println!("put to storage success, path is {:?}", path);

    // 1. Create external table
    let sql = format!(
        r#"
        CREATE EXTERNAL TABLE users (
            id INT,
            name VARCHAR,
            amount DECIMAL(10,2)
        )
        STORED AS CSV
        LOCATION '{}://{}'
        "#,
        "minio", "demo/demo/test1.csv"
    );
    db.query(&sql)
        .await
        .context("create external table error")?;

    println!("create external table success");

    // 2. Query data
    let result = db
        .query(
            r#"
            SELECT name, SUM(amount) as total_amount 
            FROM users 
            GROUP BY name 
            HAVING SUM(amount) > 100
            ORDER BY total_amount DESC
            "#,
        )
        .await?;

    // Show results
    result.clone().show().await?;
    println!("query data success");

    // 3. Export results to a new CSV
    db.export_to_storage(result, "minio", "demo/high_value_users.csv", "csv")
        .await
        .context("export to storage error")?;

    let location = Path::from_url_path("demo/high_value_users.csv").unwrap();
    let result = store.get(&location).await?;
    let result_bytes = result.bytes().await?;
    println!(
        "get from storage success, result is {:?}",
        String::from_utf8_lossy(&result_bytes)
    );
    assert!(result_bytes.len() > 0);
    assert_eq!(
        String::from_utf8_lossy(&result_bytes),
        "name,total_amount\nCharlie,350.25\nBob,200.75\nAlice,100.50\n"
    );

    Ok(())
}

#[tokio::test]
/// Example 2: Read-write operations with memory table
/// 1. Create memory table
/// 2. Load data from external source
/// 3. Modify data
/// 4. Export results
async fn example_readwrite_operations() -> anyhow::Result<()> {
    let db = DB::<()>::new("example_db");
    // 1. Create memory table
    db.query(
        r#"
        CREATE TABLE users_memory (
            id INT,
            name VARCHAR,
            amount DECIMAL(10,2)
        )
        "#,
    )
    .await?;

    // 2. Load data from external CSV
    let input_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("data")
        .join("test1.csv");

    // First create external table
    let sql = format!(
        r#"
    CREATE EXTERNAL TABLE users_external (
        id INT,
        name VARCHAR,
        amount DECIMAL(10,2)
    )
    STORED AS CSV
    LOCATION 'file://{}'"#,
        input_path.to_string_lossy()
    );
    db.query(&sql).await?;
    println!("create external table success");
    db.query("SELECT * FROM users_external")
        .await?
        .show()
        .await?;

    // Then insert from external table
    db.query(
        r#"
    INSERT INTO users_memory 
    SELECT * FROM users_external"#,
    )
    .await
    .context("insert from external table error")?
    .collect()
    .await?;
    println!("insert from external table success");

    db.query("SELECT * FROM users_memory").await?.show().await?;

    // // 3. Modify data
    // 不支持 UPDATE，DML 只支持 COPY 和 INSERT
    // // Add 10% bonus to all amounts
    // db.query(
    //     r#"
    //     UPDATE users_memory
    //     SET amount = amount * 1.1 WHERE id = 1
    //     "#,
    // )
    // .await?
    // .collect()
    // .await?;

    // Insert new record
    db.query(
        r#"
        INSERT INTO users_memory (id, name, amount)
        VALUES (4, 'David', 500.00)
        "#,
    )
    .await?
    .collect()
    .await?;

    // Query modified data
    let result = db.query("SELECT * FROM users_memory ORDER BY id").await?;

    // Show results
    result.clone().show().await?;
    println!("query data success");

    Ok(())
}
