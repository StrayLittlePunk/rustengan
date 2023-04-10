use std::collections::HashMap;
use std::io::{StdoutLock, Write};

use anyhow::Context;
use serde::{Deserialize, Serialize};

use rustengan::*;

fn main() -> anyhow::Result<()> {
    main_loop::<_, BroadcastNode, _>(())?;
    Ok(())
}

struct BroadcastNode {
    id: usize,
    node_id: String,
    messages: Vec<usize>,
}
impl Node<(), Payload> for BroadcastNode {
    fn from_init(_: (), init: Init) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(BroadcastNode {
            id: 1,
            node_id: init.node_id,
            messages: Vec::new(),
        })
    }
    fn step(&mut self, input: Message<Payload>, output: &mut StdoutLock) -> anyhow::Result<()> {
        let mut reply = input.into_reply(Some(&mut self.id));
        match reply.body.payload {
            Payload::Broadcast { message } => {
                self.messages.push(message);
                reply.body.payload = Payload::BroadcastOk;
                serde_json::to_writer(&mut *output, &reply)
                    .context("serialze repsonse to broadcast")?;
                output.write_all(b"\n").context("write \\n failed")?;
            }
            Payload::Read => {
                reply.body.payload = Payload::ReadOk {
                    messages: self.messages.clone(),
                };
                serde_json::to_writer(&mut *output, &reply).context("serialze repsonse to read")?;
                output.write_all(b"\n").context("write \\n failed")?;
            }
            Payload::Topology { topology: _ } => {
                reply.body.payload = Payload::TopologyOk;
                serde_json::to_writer(&mut *output, &reply)
                    .context("serialze repsonse to topology")?;
                output.write_all(b"\n").context("write \\n failed")?;
            }
            Payload::BroadcastOk | Payload::TopologyOk | Payload::ReadOk { .. } => (),
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Broadcast {
        message: usize,
    },
    BroadcastOk,
    Read,
    ReadOk {
        messages: Vec<usize>,
    },
    Topology {
        topology: HashMap<String, Vec<String>>,
    },
    TopologyOk,
}
