use crate::{Entry, EntryId, LogManager, SegmentReader, WriteAheadLog};
use std::io::{self};
use std::sync::{Arc, Mutex};

pub struct WriteAheadLogManager {
    pub log: WriteAheadLog,
    pub checkpointer: LoggingCheckpointer,
}

impl WriteAheadLogManager {
    pub fn new() -> Self {
        let checkpointer = LoggingCheckpointer::new();
        let log = WriteAheadLog::recover("my-log", checkpointer.clone()).unwrap();
        WriteAheadLogManager { log, checkpointer }
    }

    pub fn write_data(&mut self, data: Vec<u8>) {
        let mut writer = self.log.begin_entry().unwrap();
        let _record = writer.write_chunk(&data).unwrap();
        writer.commit().unwrap();
    }
}
#[derive(Debug, Clone)]
pub struct LoggingCheckpointer {
    recovered_data: Arc<Mutex<Vec<(EntryId, Vec<String>)>>>,
}

impl LoggingCheckpointer {
    pub fn new() -> Self {
        LoggingCheckpointer {
            recovered_data: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn get_recovered_data(&self) -> Vec<(EntryId, Vec<String>)> {
        self.recovered_data.lock().unwrap().clone()
    }
}

impl LogManager for LoggingCheckpointer {
    fn recover(&mut self, entry: &mut Entry<'_>) -> io::Result<()> {
        if let Some(all_chunks) = entry.read_all_chunks()? {
            let all_chunks = all_chunks
                .into_iter()
                .map(String::from_utf8)
                .collect::<Result<Vec<String>, _>>()
                .expect("invalid utf-8");

            println!(
                "LoggingCheckpointer::recover(entry_id: {:?}, data: {:?})",
                entry.id(),
                all_chunks,
            );

            // Store the recovered data
            self.recovered_data
                .lock()
                .unwrap()
                .push((entry.id(), all_chunks));
        } else {
            // This entry wasn't completely written. This could happen if a
            // power outage or crash occurs while writing an entry.
        }

        Ok(())
    }

    fn checkpoint_to(
        &mut self,
        last_checkpointed_id: EntryId,
        _checkpointed_entries: &mut SegmentReader,
        _wal: &WriteAheadLog,
    ) -> io::Result<()> {
        println!("LoggingCheckpointer::checkpoint_to({last_checkpointed_id:?}");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_manager() {
        let mut m = WriteAheadLogManager::new();
        m.write_data("hello world".as_bytes().to_vec());

        let recovered_data = m.checkpointer.get_recovered_data();
        assert_eq!(recovered_data[0].1, vec!["hello world".to_string()]);
    }
    #[test]
    fn test_recovery() {
        let checkpointer = LoggingCheckpointer::new();
        let log = WriteAheadLog::recover("my-log", checkpointer.clone()).unwrap();

        // Write some data
        let mut writer = log.begin_entry().unwrap();
        let record = writer
            .write_chunk("this is the first entry".as_bytes())
            .unwrap();
        writer.commit().unwrap();
        drop(log);

        // Recover the log
        let log = WriteAheadLog::recover("my-log", checkpointer.clone()).unwrap();

        // Get the recovered data
        let recovered_data = checkpointer.get_recovered_data();

        assert_eq!(recovered_data.len(), 1);
        assert_eq!(
            recovered_data[0].1,
            vec!["this is the first entry".to_string()]
        );

        // Cleanup
        drop(log);
    }

    use std::io::Read;
    #[test]
    fn test_loop_write() {
        let checkpointer = LoggingCheckpointer::new();
        let log = WriteAheadLog::recover("my-log", checkpointer).unwrap();

        for i in 0..10 {
            let mut writer = log.begin_entry().unwrap();
            let record = writer
                .write_chunk("this is the first entry".as_bytes())
                .unwrap();
            writer.commit().unwrap();
        }
        let mut writer = log.begin_entry().unwrap();
        let record = writer
            .write_chunk("this is the checkpoint".as_bytes())
            .unwrap();
        writer.commit().unwrap();

        // recover from checkpoint
        let checkpointer = LoggingCheckpointer::new();
        let log = WriteAheadLog::recover("my-log", checkpointer).unwrap();
        // We can use the previously returned DataRecord to read the original data.
        let mut reader = log.read_at(record.position).unwrap();
        let mut buffer = vec![0; usize::try_from(record.length).unwrap()];
        reader.read_exact(&mut buffer).unwrap();
        println!(
            "Data read from log: {}",
            String::from_utf8(buffer).expect("invalid utf-8")
        );
        // reset checkpoint
        log.checkpoint_active().unwrap();
        // Cleanup
        drop(reader);
    }
}
