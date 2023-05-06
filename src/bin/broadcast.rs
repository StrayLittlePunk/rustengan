use std::collections::{HashMap, HashSet};
use std::io::StdoutLock;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use anyhow::Context;
use rand::Rng;
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
    gossip_waker: Arc<(Mutex<bool>, Condvar)>, //msg_communicated: HashMap<usize, HashSet<usize>>,
    gossip_delta: usize,
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
        let con_pair = Arc::new((Mutex::new(false), Condvar::new()));
        let clone_cvar = con_pair.clone();
        std::thread::spawn(move || loop {
            let (lock, cvar) = &*clone_cvar;
            let need_gossip =
                match cvar.wait_timeout(lock.lock().unwrap(), Duration::from_millis(300)) {
                    Ok((mut g, r)) => {
                        if *g || r.timed_out() {
                            *g = false;
                            true
                        } else {
                            false
                        }
                    }
                    Err(e) => panic!("lock poison error: {e}"),
                };
            if need_gossip {
                if let Err(_) = tx.send(Event::Injected(InjectedPayload::Gossip)) {
                    break;
                }
            }
        });
        Ok(BroadcastNode {
            gossip_delta: 0,
            gossip_waker: con_pair,
            id: 1,
            node_id: init.node_id,
            messages: HashSet::new(),
            known: init
                .node_ids
                .into_iter()
                .map(|nid| (nid, HashSet::new()))
                .collect(),
            neighborhood: Default::default(),
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
                        let topology_length = topology.len();
                        // 找到不是邻居的邻居，随机抽取 ratio(17.min(not_neighbor.len()), not_neighbor.len()) 作为新的邻居，如果邻居太多就会传播泛洪,所以要小于节点数的一半
                        reply.body.payload = Payload::TopologyOk;
                        self.neighborhood = topology.remove(&self.node_id).unwrap_or_else(|| {
                            panic!("no topology given for node {}", self.node_id)
                        });
                        // eprintln!("before neighborhood: {:?}", self.neighborhood);
                        self.neighborhood.iter().for_each(|c| {
                            let _ = topology.remove(c);
                        });
                        let mut rng = rand::thread_rng();
                        let remain_topology_length = topology.len();
                        self.neighborhood.extend(topology.into_keys().filter(|_| {
                            rng.gen_ratio(
                                8.min(remain_topology_length) as u32,
                                remain_topology_length as u32,
                            )
                        }));
                        self.neighborhood.shrink_to(topology_length / 2);

                        //eprintln!("neighborhood: {:?}", self.neighborhood);
                        reply
                            .send(output)
                            .context("serialze repsonse to topology")?;
                    }
                    Payload::Gossip { seen } => {
                        // eprintln!("gossip {}", reply.dst);
                        self.known
                            .get_mut(&reply.dst)
                            .expect("got gossip from unknown node")
                            .extend(seen.iter().copied());
                        let before_msgs_length = self.messages.len();
                        self.messages.extend(seen);
                        // eprintln!("message length: {}", self.messages.len());
                        if self.messages.len() - before_msgs_length >= self.gossip_delta {
                            self.gossip_delta = self.messages.len() - before_msgs_length;
                            *self.gossip_waker.0.lock().unwrap() = true;
                            self.gossip_waker.1.notify_one();
                        }
                    }
                    Payload::GossipOk
                    | Payload::BroadcastOk
                    | Payload::TopologyOk
                    | Payload::ReadOk { .. } => (),
                }
            }
            Event::Injected(InjectedPayload::Gossip) => self.gossip(output)?,
            Event::EOF => (),
        }
        Ok(())
    }
}

impl BroadcastNode {
    fn gossip(&mut self, output: &mut StdoutLock) -> anyhow::Result<()> {
        for n in &self.neighborhood {
            let knows_to_n = &self.known[n];
            let (already_known, mut notify_of): (HashSet<_>, HashSet<_>) = self
                .messages
                .iter()
                .copied()
                .partition(|m| knows_to_n.contains(m));
            // eprintln!("notify of {}/{}", notify_of.len(), self.messages.len());
            // if we know that n knows m, we don't tell n that we know m
            // send us m for all eternity, so
            // include a couple of extra messages to let them know that we know they know
            // 邻居较少，而且网络带宽费贵的情况下，就增加一次传输携带大数据包，当已知数据的量很大的时候，最大附带1/3的数据，当数据量小的时候就全部携带,最多带30条数据
            let mut rng = rand::thread_rng();
            notify_of.extend(already_known.iter().filter(|_| {
                rng.gen_ratio(
                    30.min(already_known.len()).max(already_known.len() / 3) as u32,
                    already_known.len() as u32,
                )
            }));

            Message {
                src: self.node_id.clone(),
                dst: n.clone(),
                body: Body {
                    id: None,
                    in_reply_to: None,
                    payload: Payload::Gossip { seen: notify_of },
                },
            }
            .send(&mut *output)
            .with_context(|| format!("gossip to {n}"))?;
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
