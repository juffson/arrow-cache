use arrow::datatypes::{DataType, Field, Schema};
use prost_reflect::{DescriptorPool, MessageDescriptor};

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
        let file_descriptor_set = include_bytes!("path/to/your/compiled.proto.bin");
        pool.add_file_descriptor_set(file_descriptor_set).unwrap();

        let message_descriptor = pool.get_message_by_name("TradeData").unwrap();
        let schema = create_schema_from_proto(&message_descriptor);

        println!("Created Arrow Schema: {:?}", schema);
    }
}
