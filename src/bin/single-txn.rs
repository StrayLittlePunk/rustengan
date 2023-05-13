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
                for (op, key, value) in txn {
                    if op == "r" {
                        let v = self.storage.get(&key).cloned();
                        result.push((op, key, v));
                    } else if op == "w" {
                        self.storage.insert(key, value.unwrap());
                        result.push((op, key, value));
                    }
                }
                reply.body.payload = Payload::TxnOk { txn: result };
            }
            Payload::Error { code, text } => {
                eprintln!("kafka node step call error({code}): {text}");
                return Ok(());
            }
            Payload::Unknown | Payload::TxnOk { .. } => {
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
    #[default]
    Unknown,
    Txn {
        // op key value
        txn: Vec<(String, u64, Option<u64>)>,
    },
    TxnOk {
        // op key value
        txn: Vec<(String, u64, Option<u64>)>,
    },
    Error {
        code: u8,
        text: String,
    },
}
