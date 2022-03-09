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

impl CommandService for Hmset {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        let mut pairs: Vec<Kvpair> = Vec::new();
        for pair in self.pairs {
            match store.set(&self.table, &pair.key, pair.value.unwrap_or_default()) {
                Ok(Some(v)) => pairs.push(Kvpair::new(pair.key, v)),
                Ok(None) => pairs.push(Kvpair::new(pair.key, Value::default())),
                Err(e) => return e.into(),
            }
        }
        pairs.into()
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

impl CommandService for Hmget {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        let mut pairs: Vec<Kvpair> = Vec::new();
        for key in self.keys {
            match store.get(&self.table, &key) {
                Ok(Some(v)) => pairs.push(Kvpair::new(key, v)),
                _ => pairs.push(Kvpair::new(key, Value::default())),
            }
        }
        pairs.into()
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

impl CommandService for Hmdel {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        let mut pairs: Vec<Kvpair> = Vec::new();
        for key in self.keys {
            match store.del(&self.table, &key) {
                Ok(Some(v)) => pairs.push(Kvpair::new(key, v)),
                Ok(None) => pairs.push(Kvpair::new(key, Value::default())),
                Err(e) => return e.into(),
            }
        }
        pairs.into()
    }
}

impl CommandService for Hexist {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        match store.contains(&self.table, &self.key) {
            Ok(v) => Kvpair::new(self.key, v.into()).into(),
            Err(e) => e.into(),
        }
    }
}

impl CommandService for Hmexists {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        let mut pairs: Vec<Kvpair> = Vec::new();
        for key in self.keys {
            match store.contains(&self.table, &key) {
                Ok(v) => pairs.push(Kvpair::new(key, v.into())),
                Err(e) => return e.into(),
            }
        }

        pairs.into()
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
    fn hmset_should_work() {
        let store = MemTable::new();

        dispatch(CommandRequest::new_hset("t1", "k1", 11.into()), &store);

        let cmd = CommandRequest::new_hmset(
            "t1",
            vec![
                Kvpair::new("k1", 1.into()),
                Kvpair::new("k2", 2.into()),
                Kvpair::new("k3", 3.into()),
            ],
        );

        let res = dispatch(cmd, &store);
        assert_res_ok(
            res,
            &[],
            &[
                Kvpair::new("k1", 11.into()),
                Kvpair::new("k2", Value::default()),
                Kvpair::new("k3", Value::default()),
            ],
        );
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
    fn hmget_should_work() {
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

        let cmd = CommandRequest::new_hmget("t1", vec!["k1".into(), "k2".into()]);

        let res = dispatch(cmd, &store);
        assert_res_ok(
            res,
            &[],
            &[
                Kvpair::new("k1", "v11".into()),
                Kvpair::new("k2", "v2".into()),
            ],
        )
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
    fn hdel_should_work() {
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

    #[test]
    fn hmdel_should_work() {
        let store = MemTable::new();
        let cmds = vec![
            CommandRequest::new_hset("t1", "k1", "v1".into()),
            CommandRequest::new_hset("t1", "k2", "v2".into()),
            CommandRequest::new_hset("t1", "k3", 3.into()),
        ];
        for cmd in cmds {
            dispatch(cmd, &store);
        }

        let cmd = CommandRequest::new_hmdel(
            "t1",
            vec!["k1".into(), "k2".into(), "k3".into(), "k4".into()],
        );
        let res = dispatch(cmd, &store);
        assert_res_ok(
            res,
            &[],
            &[
                Kvpair::new("k1", "v1".into()),
                Kvpair::new("k2", "v2".into()),
                Kvpair::new("k3", 3.into()),
                Kvpair::new("k4", Value::default()),
            ],
        )
    }

    #[test]
    fn hexist_should_work() {
        let store = MemTable::new();
        let res = dispatch(CommandRequest::new_hexist("t1", "k1"), &store);
        assert_res_ok(res, &[], &[Kvpair::new("k1", false.into())]);

        dispatch(CommandRequest::new_hset("t1", "k1", 1.into()), &store);
        let res = dispatch(CommandRequest::new_hexist("t1", "k1"), &store);
        assert_res_ok(res, &[], &[Kvpair::new("k1", true.into())]);
    }

    #[test]
    fn hmexist_should_work() {
        let store = MemTable::new();
        let cmds = vec![
            CommandRequest::new_hset("t1", "k1", "v1".into()),
            CommandRequest::new_hset("t1", "k3", 3.into()),
        ];
        for cmd in cmds {
            dispatch(cmd, &store);
        }

        let cmd = CommandRequest::new_hmexist("t1", vec!["k1".into(), "k2".into(), "k3".into()]);
        let res = dispatch(cmd, &store);

        assert_res_ok(
            res,
            &[],
            &[
                Kvpair::new("k1", true.into()),
                Kvpair::new("k2", false.into()),
                Kvpair::new("k3", true.into()),
            ],
        )
    }

    fn dispatch(cmd: CommandRequest, store: &impl Storage) -> CommandResponse {
        match cmd.request_data.unwrap() {
            RequestData::Hset(cmd) => cmd.execute(store),
            RequestData::Hget(cmd) => cmd.execute(store),
            RequestData::Hdel(cmd) => cmd.execute(store),
            RequestData::Hgetall(cmd) => cmd.execute(store),
            RequestData::Hmget(cmd) => cmd.execute(store),
            RequestData::Hmset(cmd) => cmd.execute(store),
            RequestData::Hmdel(cmd) => cmd.execute(store),
            RequestData::Hexist(cmd) => cmd.execute(store),
            RequestData::Hmexists(cmd) => cmd.execute(store),
        }
    }
}
