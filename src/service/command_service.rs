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

#[cfg(test)]
mod tests {

    use super::*;
    use crate::{command_request::RequestData, memory::MemTable};

    #[test]
    fn hset_should_work() {
        let store = MemTable::new();
        let cmd = CommandRequest::new_hset("t1", "hello", "world".into());
        let res = dispatch(cmd.clone(), &store);
        assert_res_ok(res, &[Value::default()], &[]);

        let res = dispatch(cmd.clone(), &store);
        assert_res_ok(res, &["world".into()], &[]);
    }

    fn dispatch(cmd: CommandRequest, store: &impl Storage) -> CommandResponse {
        match cmd.request_data.unwrap() {
            RequestData::Hset(cmd) => cmd.execute(store),
            _ => todo!(),
        }
    }

    fn assert_res_ok(mut res: CommandResponse, values: &[Value], pairs: &[Kvpair]) {
        res.pairs.sort_by(|a, b| a.partial_cmp(b).unwrap());
        assert_eq!(res.status, 200);
        assert_eq!(res.message, "");
        assert_eq!(res.values, values);
        assert_eq!(res.pairs, pairs)
    }
}
