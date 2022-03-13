use crate::{KvError, Kvpair, Storage, Value};
use sled::{Db, IVec};
use std::path::Path;
use std::str;

#[derive(Debug)]
pub struct SledDb(Db);

impl SledDb {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self(sled::open(path).unwrap())
    }

    pub fn get_full_key(prefix: &str, key: &str) -> String {
        format!("{}:{}", prefix, key)
    }

    fn get_table_prefix(table: &str) -> String {
        table.to_string()
    }
}

impl Storage for SledDb {
    fn get(&self, table: &str, key: &str) -> Result<Option<Value>, KvError> {
        let full_key = SledDb::get_full_key(table, key);
        let result = self.0.get(full_key)?;
        result.map(|v| v.as_ref().try_into()).transpose()
    }

    fn get_all(&self, table: &str) -> Result<Vec<Kvpair>, KvError> {
        let prefix = SledDb::get_table_prefix(table);
        let result = self.0.scan_prefix(prefix).map(|v| v.into()).collect();

        Ok(result)
    }

    fn get_iter(&self, table: &str) -> Result<Box<dyn Iterator<Item = Kvpair>>, KvError> {
        let prefix = SledDb::get_table_prefix(table);
        let iter = self.0.scan_prefix(prefix).into_iter().map(|v| v.into());
        Ok(Box::new(iter))
    }

    fn set(&self, table: &str, key: &str, value: Value) -> Result<Option<Value>, KvError> {
        let full_key = SledDb::get_full_key(table, key);
        let data: Vec<u8> = value.try_into()?;

        let result = self
            .0
            .insert(full_key, data)?
            .map(|v| v.as_ref().try_into());
        result.transpose()
    }

    fn contains(&self, table: &str, key: &str) -> Result<bool, KvError> {
        let full_key = SledDb::get_full_key(table, key);
        Ok(self.0.contains_key(full_key)?)
    }

    fn del(&self, table: &str, key: &str) -> Result<Option<Value>, KvError> {
        let full_key = SledDb::get_full_key(table, key);
        let result = self.0.remove(full_key)?.map(|v| v.as_ref().try_into());
        result.transpose()
    }
}

impl From<Result<(IVec, IVec), sled::Error>> for Kvpair {
    fn from(v: Result<(IVec, IVec), sled::Error>) -> Self {
        match v {
            Ok((k, v)) => match v.as_ref().try_into() {
                Ok(v) => Kvpair::new(ivec_to_key(k.as_ref()), v),
                Err(_) => Kvpair::default(),
            },
            _ => Kvpair::default(),
        }
    }
}

fn ivec_to_key(ivec: &[u8]) -> &str {
    let s = str::from_utf8(ivec).unwrap();
    let mut iter = s.split(':');
    iter.next();
    iter.next().unwrap()
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use crate::storage::{test_basi_interface, test_get_all};

    use super::*;

    #[test]
    fn sled_basic_interface_should_work() {
        let dir = tempdir().unwrap();
        let store = SledDb::new(dir);
        test_basi_interface(store)
    }

    #[test]
    fn sleddb_get_all_should_work() {
        let dir = tempdir().unwrap();
        let store = SledDb::new(dir);
        test_get_all(store);
    }
}
