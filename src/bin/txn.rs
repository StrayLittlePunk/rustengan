use std::collections::HashMap;
use std::io::StdoutLock;
use std::sync::mpsc::Receiver;

use serde::{Deserialize, Serialize};

use rustengan::*;

fn main() -> Result<()> {
    main_loop::<_, TxnNode, _, _>(())?;
    Ok(())
}

struct TxnNode {
    id: usize,
    #[allow(unused)]
    node_id: String,
    node_ids: Vec<String>,
    storage: HashMap<u64, u64>,
}

impl Node<(), Payload> for TxnNode {
    fn from_init(_: (), init: Init, _: std::sync::mpsc::Sender<Event<Payload>>) -> Result<Self>
    where
        Self: Sized,
    {
        Ok(TxnNode {
            id: 1,
            node_id: init.node_id,
            storage: HashMap::new(),
            node_ids: init.node_ids,
        })
    }

    fn step(
        &mut self,
        input: Event<Payload>,
        output: &mut StdoutLock,
        _: &Receiver<Event<Payload>>,
    ) -> Result<()> {
        let Event::Message(input) = input else {
            panic!("got injected event when there's no event injection");
        };
        let mut reply = input.into_reply(Some(&mut self.id));
        match reply.body.payload {
            Payload::Txn { txn } => {
                let mut result = Vec::new();
                let mut changed = Vec::new();
                for (op, key, value) in txn {
                    if op == "r" {
                        let v = self.storage.get(&key).cloned();
                        result.push((op, key, v));
                    } else if op == "w" {
                        self.storage.insert(key, value.unwrap());
                        result.push((op, key, value));
                        changed.push((key, value.unwrap()));
                    }
                }
                // fan out to other nodes
                for node in self.node_ids.iter() {
                    if node == &self.node_id {
                        continue;
                    }
                    let mut message =
                        Message::kv_message(&self.node_id, node, Some(&mut self.id), None);
                    message.body.payload = Payload::Sync {
                        changed: changed.clone(),
                    };
                    message.send(output)?;
                }
                reply.body.payload = Payload::TxnOk { txn: result };
            }
            Payload::Sync { changed } => {
                for (k, v) in changed {
                    self.storage.insert(k, v);
                }
                reply.body.payload = Payload::SyncOk;
            }
            Payload::Error { code, text } => {
                eprintln!("kafka node step call error({code}): {text}");
                return Ok(());
            }
            Payload::SyncOk => return Ok(()),
            Payload::TxnOk { .. } => {
                return Err(GanError::Normal(
                    "should not exist invalid response".to_string(),
                ));
            }
        }
        reply.send(output)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Payload {
    Txn {
        // op key value
        txn: Vec<(String, u64, Option<u64>)>,
    },
    TxnOk {
        // op key value
        txn: Vec<(String, u64, Option<u64>)>,
    },
    Sync {
        changed: Vec<(u64, u64)>,
    },
    #[default]
    SyncOk,
    Error {
        code: u8,
        text: String,
    },
}
