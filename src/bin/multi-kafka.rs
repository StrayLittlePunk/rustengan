use std::collections::HashMap;
use std::io::StdoutLock;
use std::sync::mpsc::{Receiver, Sender};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use rustengan::*;

fn main() -> Result<()> {
    main_loop::<_, KafkaNode, _, _>(())?;
    Ok(())
}

struct KafkaNode {
    id: usize,
    node_id: String,
    storage: LinKv,
    tx: Sender<Event<Payload>>,
}

impl Node<(), Payload> for KafkaNode {
    fn from_init(_: (), init: Init, tx: Sender<Event<Payload>>) -> Result<Self>
    where
        Self: Sized,
    {
        Ok(KafkaNode {
            id: 1,
            node_id: init.node_id,
            storage: LinKv {
                stash_event: Vec::new(),
            },
            tx,
        })
    }

    fn step(
        &mut self,
        input: Event<Payload>,
        output: &mut StdoutLock,
        rx: &Receiver<Event<Payload>>,
    ) -> Result<()> {
        let Event::Message(input) = input else {
            panic!("got injected event when there's no event injection");
        };
        let mut reply = input.into_reply(Some(&mut self.id));
        let rt = Runtime {
            id: &mut self.id,
            node_id: &self.node_id,
            rx,
            writer: output,
            in_reply_to: None,
        };
        let storage = &mut self.storage;
        match reply.body.payload {
            Payload::Send { key, msg } => {
                let offset = storage.send(rt, key, msg)?;
                reply.body.payload = Payload::SendOk { offset };
            }
            Payload::Poll { offsets } => {
                let msgs = self.storage.poll(rt, offsets)?;
                reply.body.payload = Payload::PollOk { msgs };
            }
            Payload::CommitOffsets { offsets } => {
                self.storage.commit_offsets(rt, offsets)?;
                reply.body.payload = Payload::CommitOffsetsOk;
            }
            Payload::ListCommittedOffsets { keys } => {
                let committed_offsets = storage.list_committed_offsets(rt, keys);
                reply.body.payload = Payload::ListCommittedOffsetsOk {
                    offsets: committed_offsets,
                };
            }
            Payload::Error { code, text } => {
                eprintln!("kafka node step call error({code}): {text}");
                return Ok(());
            }
            Payload::SendOk { .. }
            | Payload::Cas { .. }
            | Payload::KvRead { .. }
            | Payload::CasOk
            | Payload::ReadOk { .. }
            | Payload::Write { .. }
            | Payload::WriteOk
            | Payload::CommitOffsetsOk
            | Payload::ListCommittedOffsetsOk { .. }
            | Payload::PollOk { .. } => {
                return Err(GanError::Normal(
                    "should not exist invalid response for step".to_string(),
                ));
            }
        }
        reply.send(output)?;
        for event in self.storage.stash_event.drain(..) {
            self.tx.send(Event::Message(event))?;
        }
        Ok(())
    }
}

struct LinKv {
    stash_event: Vec<Message<Payload>>,
}

const KV_NAME: &str = "lin-kv";
const PREFIX_COMMIT: &str = "commit";
const PREFIX_LATEST: &str = "latest";
const PREFIX_ENTRY: &str = "entry";

impl LinKv {
    fn send(&mut self, mut rt: Runtime<Payload>, key: String, value: u64) -> Result<u64> {
        let latest_key = format!("{}_{}", PREFIX_LATEST, key);
        let mut offset = match self.read(&mut rt, &latest_key) {
            Ok(e) => e,
            Err(GanError::KeyNotExist) => 0,
            Err(e) => return Err(e),
        };
        let offset = loop {
            match self.compare_exchange(&mut rt, latest_key.as_str(), offset, offset + 1, true) {
                Ok(_) => break offset,
                Err(GanError::PreconditionFailed) => {
                    offset += 1;
                }
                Err(e) => return Err(e),
            }
        };
        self.write(
            &mut rt,
            format!("{}_{}_{}", PREFIX_ENTRY, key, offset),
            value,
        )?;
        Ok(offset)
    }

    fn poll(
        &mut self,
        mut rt: Runtime<Payload>,
        offsets: HashMap<String, u64>,
    ) -> Result<HashMap<String, Vec<(u64, u64)>>> {
        let mut result = HashMap::new();
        if offsets.is_empty() {
            return Ok(result);
        }
        for (key, ofs) in offsets.into_iter() {
            let mut key_offsets = Vec::new();
            for o in ofs.. {
                let value_key = format!("{}_{}_{}", PREFIX_ENTRY, key, o);
                let Ok(value) = self.read(&mut rt, &value_key) else {
                    break;
                };
                key_offsets.push((o, value));
            }
            if !key_offsets.is_empty() {
                result.insert(key, key_offsets);
            }
        }
        Ok(result)
    }

    fn commit_offsets(
        &mut self,
        mut rt: Runtime<Payload>,
        offsets: HashMap<String, u64>,
    ) -> Result<()> {
        if offsets.is_empty() {
            return Ok(());
        }
        for (key, ofs) in offsets.into_iter() {
            self.write(&mut rt, format!("{}_{}", PREFIX_COMMIT, key), ofs)?;
        }
        Ok(())
    }

    fn list_committed_offsets(
        &mut self,
        mut rt: Runtime<Payload>,
        keys: Vec<String>,
    ) -> HashMap<String, u64> {
        if keys.is_empty() {
            return HashMap::new();
        }
        keys.into_iter()
            .filter_map(|k| {
                let offset = self
                    .read(&mut rt, format!("{}_{}", PREFIX_COMMIT, k).as_str())
                    .ok();
                Some(k).zip(offset)
            })
            .collect()
    }
}

impl KV for LinKv {
    type Value = u64;
    type Payload = Payload;
    fn read(&mut self, rt: &mut Runtime<'_, '_, Self::Payload>, key: &str) -> Result<Self::Value> {
        let payload = Payload::KvRead {
            key: key.to_string(),
        };
        let mut message = Message::kv_message(rt.node_id, KV_NAME, Some(rt.id), rt.in_reply_to);
        message.body.payload = payload;
        message.send(rt.writer)?;
        let timeout = Duration::from_secs(1);
        let now = Instant::now();
        loop {
            let Event::Message(input) = rt.rx.recv()? else {
                panic!("got injected event when there's no event injection");
            };
            match input.body.payload {
                Payload::ReadOk { value } => {
                    rt.in_reply_to = input.body.id;
                    return Ok(value);
                }
                Payload::Error { code, .. } if code == 20 => {
                    rt.in_reply_to = input.body.id;
                    return Err(GanError::KeyNotExist);
                }
                Payload::Error { code, text } => {
                    rt.in_reply_to = input.body.id;
                    return Err(GanError::Rpc { code, text });
                }
                Payload::Send { .. }
                | Payload::Poll { .. }
                | Payload::CommitOffsets { .. }
                | Payload::ListCommittedOffsets { .. } => {
                    self.stash_event.push(input);
                }
                _ => {
                    return Err(GanError::Normal(
                        "should not exist invalid response".to_string(),
                    ))
                }
            }
            if now.elapsed() >= timeout {
                return Err(GanError::Normal("wait response timeout".to_string()));
            }
        }
    }

    fn write(
        &mut self,
        rt: &mut Runtime<'_, '_, Self::Payload>,
        key: String,
        value: Self::Value,
    ) -> Result<()> {
        let payload = Payload::Write { key, value };
        let mut message = Message::kv_message(rt.node_id, KV_NAME, Some(rt.id), rt.in_reply_to);
        message.body.payload = payload;
        message.send(rt.writer)?;
        let timeout = Duration::from_secs(1);
        let now = Instant::now();
        loop {
            let Event::Message(input) = rt.rx.recv()? else {
                panic!("got injected event when there's no event injection");
            };
            match input.body.payload {
                Payload::WriteOk => {
                    rt.in_reply_to = input.body.id;
                    return Ok(());
                }
                Payload::Error { code, text } => {
                    rt.in_reply_to = input.body.id;
                    return Err(GanError::Rpc { code, text });
                }
                Payload::Send { .. }
                | Payload::Poll { .. }
                | Payload::CommitOffsets { .. }
                | Payload::ListCommittedOffsets { .. } => {
                    self.stash_event.push(input);
                }
                _ => {
                    return Err(GanError::Normal(
                        "should not exist invalid response".to_string(),
                    ))
                }
            }
            if now.elapsed() >= timeout {
                return Err(GanError::Normal("wait response timeout".to_string()));
            }
        }
    }

    fn compare_exchange(
        &mut self,
        rt: &mut Runtime<'_, '_, Self::Payload>,
        key: &str,
        from: Self::Value,
        to: Self::Value,
        create_if_not_exists: bool,
    ) -> Result<()> {
        let payload = Payload::Cas {
            key: key.to_string(),
            from,
            to,
            create_if_not_exists,
        };
        let mut message = Message::kv_message(rt.node_id, KV_NAME, Some(rt.id), rt.in_reply_to);
        message.body.payload = payload;
        message.send(rt.writer)?;
        let timeout = Duration::from_secs(1);
        let now = Instant::now();
        loop {
            let Event::Message(input) = rt.rx.recv()? else {
                panic!("got injected event when there's no event injection");
            };
            match input.body.payload {
                Payload::CasOk => {
                    rt.in_reply_to = input.body.id;
                    return Ok(());
                }
                Payload::Error { code, text } => {
                    rt.in_reply_to = input.body.id;
                    // The requested operation expected some conditions to hold, and those conditions were not met.
                    if code == 22 {
                        return Err(GanError::PreconditionFailed);
                    } else if code == 20 {
                        return Err(GanError::KeyNotExist);
                    }
                    return Err(GanError::Rpc { code, text });
                }
                Payload::Send { .. }
                | Payload::Poll { .. }
                | Payload::CommitOffsets { .. }
                | Payload::ListCommittedOffsets { .. } => {
                    self.stash_event.push(input);
                }
                _ => {
                    return Err(GanError::Normal(
                        "should not exist invalid response".to_string(),
                    ))
                }
            }
            if now.elapsed() >= timeout {
                return Err(GanError::Normal("wait response timeout".to_string()));
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Payload {
    Send {
        key: String,
        msg: u64,
    },
    SendOk {
        offset: u64,
    },
    Poll {
        offsets: HashMap<String, u64>,
    },
    PollOk {
        msgs: HashMap<String, Vec<(u64, u64)>>,
    },
    CommitOffsets {
        offsets: HashMap<String, u64>,
    },
    #[default]
    CommitOffsetsOk,
    ListCommittedOffsets {
        keys: Vec<String>,
    },
    ListCommittedOffsetsOk {
        offsets: HashMap<String, u64>,
    },
    Error {
        code: u8,
        text: String,
    },
    #[serde(rename = "read")]
    KvRead {
        key: String,
    },
    ReadOk {
        value: u64,
    },
    Write {
        key: String,
        value: u64,
    },
    WriteOk,
    Cas {
        key: String,
        from: u64,
        to: u64,
        create_if_not_exists: bool,
    },
    CasOk,
}

// Both care about giving an illusion of a single copy.
//     – From the outside observer, the system should (almost)
// behave as if there’s only a single copy.
// • Linearizability cares about **time**.
// • Sequential consistency cares about **program order**.
//
// Linearizability: single-operation, single-object, real-time order
// Serializability: multi-operation, multi-object, arbitrary total order
