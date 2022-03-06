use crate::{Value, KvError, Kvpair};

pub trait Storage {
   fn get(&self, table: &str, key: &str) -> Result<Option<Value>, KvError>;

   fn set(&self, table: &str, key: &str, value: Value) -> Result<Option<Value>, KvError>;

   fn contains(&self, table: &str, key: &str) -> Result<bool, KvError>;

   fn del(&self, table: &str, key: &str) -> Result<Option<Value>, KvError>;

   fn get_all(&self, table: &str) -> Result<Vec<Kvpair>, KvError>;

   fn get_iter(&self, table: &str) -> Result<Box<dyn Iterator<Item = Kvpair>>, KvError>;
}

#[cfg(test)]
mod tests {
   use super::*;

   fn test_basi_interface(store: impl Storage) {
      let v = store.set("t1", "hello", "world".into());
      assert!(v.unwrap().is_none());

      let v1 = store.set("t1", "hello", "world1".into());
      assert_eq!(v1, Ok(Some("world".into())));

      let v = store.get("t1", "hello");
      assert_eq!(v, Ok(Some("world1".into())));

      assert!(store.get("t1", "hello1").unwrap().is_none());

      assert_eq!(store.contains("t1", "hello"), Ok(true));
      assert_eq!(store.contains("t1", "hello1"), Ok(false));
      assert_eq!(store.contains("t2", "hello"), Ok(false));

      let v = store.del("t1", "hello");
      assert_eq!(v, Ok(Some("world1".into())));

      let v = store.get("t1", "hello");
      assert!(v.unwrap().is_none());

      assert_eq!(store.del("t1", "hello1"), Ok(None));
      assert_eq!(store.del("t2", "hello"), Ok(None));
   }

   fn test_get_all(store: impl Storage) {
      store.set("t2", "k1", "v1".into()).unwrap();
      store.set("t2", "k2", "v2".into()).unwrap();

      let mut data = store.get_all("t1").unwrap();
      data.sort_by(|a, b| a.partial_cmp(b).unwrap());
      assert_eq!(
         data,
         vec![
            Kvpair::new("k1", "v1".into()),
            Kvpair::new("k2", "v2".into())
         ]
      )

   }
}
