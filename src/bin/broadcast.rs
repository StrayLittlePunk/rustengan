use std::collections::{HashMap, HashSet};
use std::io::StdoutLock;

use anyhow::Context;
use serde::{Deserialize, Serialize};

use rustengan::*;

fn main() -> anyhow::Result<()> {
    main_loop::<_, BroadcastNode, _, _>(())?;
    Ok(())
}

struct BroadcastNode {
    id: usize,
    node_id: String,
    messages: HashSet<usize>,
    neighborhood: Vec<String>,
    known: HashMap<String, HashSet<usize>>,
    //msg_communicated: HashMap<usize, HashSet<usize>>,
}

impl Node<(), Payload, InjectedPayload> for BroadcastNode {
    fn from_init(
        _: (),
        init: Init,
        tx: std::sync::mpsc::Sender<Event<Payload, InjectedPayload>>,
    ) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        std::thread::spawn(move || loop {
            std::thread::sleep(std::time::Duration::from_millis(300));
            if let Err(_) = tx.send(Event::Injected(InjectedPayload::Gossip)) {
                break;
            }
        });
        Ok(BroadcastNode {
            id: 1,
            node_id: init.node_id,
            messages: HashSet::new(),
            known: init
                .node_ids
                .into_iter()
                .map(|nid| (nid, HashSet::new()))
                .collect(),
            neighborhood: Vec::new(),
            //     msg_communicated: HashMap::new(),
        })
    }
    fn step(
        &mut self,
        input: Event<Payload, InjectedPayload>,
        output: &mut StdoutLock,
    ) -> anyhow::Result<()> {
        match input {
            Event::Message(input) => {
                let mut reply = input.into_reply(Some(&mut self.id));
                match reply.body.payload {
                    Payload::Broadcast { message } => {
                        self.messages.insert(message);
                        reply.body.payload = Payload::BroadcastOk;
                        reply
                            .send(output)
                            .context("serialze repsonse to broadcast")?;
                    }
                    Payload::Read => {
                        reply.body.payload = Payload::ReadOk {
                            messages: self.messages.iter().map(Clone::clone).collect(),
                        };
                        reply.send(output).context("serialze repsonse to read")?;
                    }
                    Payload::Topology { mut topology } => {
                        reply.body.payload = Payload::TopologyOk;
                        self.neighborhood = topology.remove(&self.node_id).unwrap_or_else(|| {
                            panic!("no topology given for node {}", self.node_id)
                        });
                        reply
                            .send(output)
                            .context("serialze repsonse to topology")?;
                    }
                    Payload::Gossip { seen } => {
                        self.messages.extend(seen);
                    }
                    Payload::GossipOk
                    | Payload::BroadcastOk
                    | Payload::TopologyOk
                    | Payload::ReadOk { .. } => (),
                }
            }
            Event::Injected(InjectedPayload::Gossip) => {
                for n in &self.neighborhood {
                    let knows_to_n = &self.known[n];
                    Message {
                        src: self.node_id.clone(),
                        dst: n.clone(),
                        body: Body {
                            id: None,
                            in_reply_to: None,
                            payload: Payload::Gossip {
                                seen: self
                                    .messages
                                    .iter()
                                    .filter(|&m| !knows_to_n.contains(m))
                                    .copied()
                                    .collect(),
                            },
                        },
                    }
                    .send(&mut *output)
                    .with_context(|| format!("gossip to {n}"))?;
                }
            }
            Event::EOF => (),
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
        messages: HashSet<usize>,
    },
    Topology {
        topology: HashMap<String, Vec<String>>,
    },
    TopologyOk,

    Gossip {
        seen: HashSet<usize>,
    },
    GossipOk,
}

enum InjectedPayload {
    Gossip,
}
