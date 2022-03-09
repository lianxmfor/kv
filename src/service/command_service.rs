use crate::*;

impl CommandService for Hset {
    fn execute(self, store: &impl storage::Storage) -> CommandResponse {
        match self.pair {
            Some(v) => match store.set(&self.table, &v.key, v.value.unwrap_or_default()) {
                Ok(Some(v)) => v.into(),
                Ok(None) => Value::default().into(),
                Err(e) => e.into(),
            },
            None => Value::default().into(),
        }
    }
}

impl CommandService for Hget {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        match store.get(&self.table, &self.key) {
            Ok(Some(v)) => v.into(),
            Ok(None) => KvError::NotFound(self.table, self.key).into(),
            Err(e) => e.into(),
        }
    }
}

impl CommandService for Hgetall {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        match store.get_all(&self.table) {
            Ok(v) => v.into(),
            Err(e) => e.into(),
        }
    }
}

impl CommandService for Hdel {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        match store.del(&self.table, &self.key) {
            Ok(Some(v)) => v.into(),
            Ok(None) => Value::default().into(),
            Err(e) => e.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command_request::RequestData;
    use crate::memory::MemTable;
    use crate::service::*;

    #[test]
    fn hset_should_work() {
        let store = MemTable::new();
        let cmd = CommandRequest::new_hset("t1", "hello", "world".into());
        let res = dispatch(cmd.clone(), &store);
        assert_res_ok(res, &[Value::default()], &[]);

        let res = dispatch(cmd.clone(), &store);
        assert_res_ok(res, &["world".into()], &[]);
    }

    #[test]
    fn hget_should_work() {
        let store = MemTable::new();
        dispatch(CommandRequest::new_hset("t1", "k1", "v1".into()), &store);

        let res = dispatch(CommandRequest::new_hget("t1", "k1"), &store);
        assert_res_ok(res, &["v1".into()], &[]);
    }

    #[test]
    fn hget_with_non_exist_key_should_return_404() {
        let store = MemTable::new();
        let cmd = CommandRequest::new_hget("t1", "k1");
        let res = dispatch(cmd, &store);
        assert_res_error(res, 404, "Not found");
    }

    #[test]
    fn hget_all_should_work() {
        let store = MemTable::new();
        let cmds = vec![
            CommandRequest::new_hset("t1", "k1", "v1".into()),
            CommandRequest::new_hset("t1", "k2", "v2".into()),
            CommandRequest::new_hset("t1", "k3", "v3".into()),
            CommandRequest::new_hset("t1", "k1", "v11".into()),
        ];

        for cmd in cmds {
            dispatch(cmd, &store);
        }

        let cmd = CommandRequest::new_hgetall("t1");
        let res = dispatch(cmd, &store);
        let pairs = &[
            Kvpair::new("k1", "v11".into()),
            Kvpair::new("k2", "v2".into()),
            Kvpair::new("k3", "v3".into()),
        ];

        assert_res_ok(res, &[], pairs);
    }

    #[test]
    fn hdel_should_workd() {
        let store = MemTable::new();
        let cmd = CommandRequest::new_hdel("t1", "k1");
        let res = dispatch(cmd, &store);
        assert_res_ok(res, &[Default::default()], &[]);

        let cmd = CommandRequest::new_hset("t1", "k1", "v1".into());
        dispatch(cmd, &store);

        let cmd = CommandRequest::new_hdel("t1", "k1");
        let res = dispatch(cmd, &store);
        assert_res_ok(res, &["v1".into()], &[]);
    }

    fn dispatch(cmd: CommandRequest, store: &impl Storage) -> CommandResponse {
        match cmd.request_data.unwrap() {
            RequestData::Hset(cmd) => cmd.execute(store),
            RequestData::Hget(cmd) => cmd.execute(store),
            RequestData::Hdel(cmd) => cmd.execute(store),
            RequestData::Hgetall(cmd) => cmd.execute(store),
            _ => todo!(),
        }
    }
}
