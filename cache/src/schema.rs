use anyhow::Context;
use arrow::datatypes::{DataType, Field, Schema};
use prost_reflect::{DescriptorPool, MessageDescriptor};
use std::path::Path;

/// create_schema_from_proto_file 从 .proto 文件创建 Arrow Schema
///
/// # Arguments
///
/// * `proto_file` - 要编译的 .proto 文件路径
/// * `message_name` - 要创建 Schema 的消息名称，注意需要包含 package 名称, 例如 `foo.bar.Message1`
///
fn create_schema_from_proto_file(proto_file: &Path, message_name: &str) -> anyhow::Result<Schema> {
    // 创建一个临时目录来存储生成的代码
    let out_dir = tempfile::tempdir()?;

    // 使用 prost-build 编译 .proto 文件
    let mut config = prost_build::Config::new();
    config.out_dir(out_dir.path());
    config.file_descriptor_set_path(out_dir.path().join("descriptor.bin"));
    config
        .compile_protos(&[proto_file], &[proto_file.parent().context("no parent")?])
        .context("compile protos")?;

    // 读取生成的文件描述符集
    let descriptor_bytes = std::fs::read(out_dir.path().join("descriptor.bin"))?;

    // 创建 DescriptorPool 并添加文件描述符集
    let pool = DescriptorPool::decode(descriptor_bytes.as_slice())?;
    // 获取指定消息的描述符
    let message_descriptor = pool
        .get_message_by_name(message_name)
        .context("get message descriptor")?;

    // 使用之前的函数创建 Schema
    Ok(create_schema_from_proto(&message_descriptor))
}

/// 从 MessageDescriptor 创建 Arrow Schema
/// TODO: support nested message
fn create_schema_from_proto(proto_descriptor: &MessageDescriptor) -> Schema {
    let fields: Vec<Field> = proto_descriptor
        .fields()
        .map(|field| {
            let name = field.name();
            println!("name is {}, kind is {:?}", name, field.kind());
            let data_type = match field.kind() {
                prost_reflect::Kind::Int32
                | prost_reflect::Kind::Sint32
                | prost_reflect::Kind::Sfixed32 => DataType::Int32,
                prost_reflect::Kind::Int64
                | prost_reflect::Kind::Sint64
                | prost_reflect::Kind::Sfixed64 => DataType::Int64,
                prost_reflect::Kind::Uint32 | prost_reflect::Kind::Fixed32 => DataType::UInt32,
                prost_reflect::Kind::Uint64 | prost_reflect::Kind::Fixed64 => DataType::UInt64,
                prost_reflect::Kind::Float => DataType::Float32,
                prost_reflect::Kind::Double => DataType::Float64,
                prost_reflect::Kind::Bool => DataType::Boolean,
                prost_reflect::Kind::String | prost_reflect::Kind::Bytes => DataType::Utf8,
                // 处理其他类型...
                _ => panic!("Unsupported protobuf type: {:?}", field.kind()),
            };
            Field::new(name, data_type, false)
        })
        .collect();

    Schema::new(fields)
}

// test
#[cfg(test)]
mod tests {

    use super::*;
    #[tokio::test]
    async fn test_create_from_proto() {
        let schema_res =
            create_schema_from_proto_file(Path::new("tests/data/basic.proto"), "foo.bar.Foo");
        assert!(schema_res.is_ok());
    }
}
