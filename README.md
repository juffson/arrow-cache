# arrow-cache

基于 DataFusion 和 Object Store 构建的分布式 SQL 缓存系统。

## Storage 相关示例

你可以查看 `cache/tests/examples.rs` 文件，里面有详细的示例。

注意
- 外部表仅支持读，不支持写入
- DML 操作仅支持 INSERT（不支持 UPDATE 和 DELETE）
- CSV 格式可以自动化猜测 schema, 但最好手动指定 schema

### 示例 1：使用外部表的只读操作

```rust
let sql = r#"
    CREATE EXTERNAL TABLE users (
    id INT,
    name VARCHAR,
    amount DECIMAL(10,2)
    )
    STORED AS CSV
    LOCATION 'minio://demo/test1.csv'
"#;
db.query(sql).await?;
// 查询数据
let result = db.query(r#"
    SELECT name, SUM(amount) as total_amount
    FROM users
    GROUP BY name
    HAVING SUM(amount) > 100
    ORDER BY total_amount DESC
"#).await?;
// 导出结果
db.export_to_storage(result, "minio", "demo/high_value_users.csv", "csv").await?;
```

这个示例展示了如何：
1. 从远程/本地 CSV 文件创建外部表
2. 查询数据（只读操作）
3. 将结果导出到远程/本地 CSV 文件


### 示例 2：使用内存表的读写操作
这个示例展示了如何：
1. 创建内存表
2. 从外部源加载数据
3. 修改数据（插入新记录）
4. 查询修改后的数据

```rust
db.query(r#"
    CREATE EXTERNAL TABLE users_external (
    id INT,
    name VARCHAR,
    amount DECIMAL(10,2)
    )
    STORED AS CSV
    LOCATION 'file:///path/to/data.csv'
"#).await?;
// 从外部表加载数据
db.query(r#"
    INSERT INTO users_memory
    SELECT FROM users_external
"#).await?;
```