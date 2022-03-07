use crate::{CommandResponse, Storage};

pub trait CommandService {
    fn execute(self, store: &impl Storage) -> CommandResponse;
}
