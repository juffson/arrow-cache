use arrow::datatypes::{DataType, Field, Schema};
use prost_reflect::{DescriptorPool, MessageDescriptor};
use std::path::Path;
fn create_schema_from_proto_file(
    proto_file: &Path,
    message_name: &str,
) -> Result<Schema, Box<dyn std::error::Error>> {
    // 创建一个临时目录来存储生成的代码
    let out_dir = tempfile::tempdir()?;

    // 使用 prost-build 编译 .proto 文件
    let mut config = prost_build::Config::new();
    config.file_descriptor_set_path(out_dir.path().join("descriptor.bin"));
    config.compile_protos(&[proto_file], &[proto_file.parent().unwrap()])?;

    // 读取生成的文件描述符集
    let descriptor_bytes = std::fs::read(out_dir.path().join("descriptor.bin"))?;

    // 创建 DescriptorPool 并添加文件描述符集
    let pool = DescriptorPool::decode(descriptor_bytes.as_slice())?;

    // 获取指定消息的描述符
    let message_descriptor = pool
        .get_message_by_name(message_name)
        .ok_or_else(|| format!("Message '{}' not found in proto file", message_name))?;

    // 使用之前的函数创建 Schema
    Ok(create_schema_from_proto(&message_descriptor))
}

fn create_schema_from_proto(proto_descriptor: &MessageDescriptor) -> Schema {
    let fields: Vec<Field> = proto_descriptor
        .fields()
        .into_iter()
        .map(|field| {
            let name = field.name();
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
        let pool = DescriptorPool::new();
        //let file_descriptor_set = include_bytes!("path/to/your/compiled.proto.bin");
        // pool.add_file_descriptor_set(file_descriptor_set).unwrap();

        let message_descriptor = pool.get_message_by_name("TradeData").unwrap();
        let schema = create_schema_from_proto(&message_descriptor);

        println!("Created Arrow Schema: {:?}", schema);
    }
}
