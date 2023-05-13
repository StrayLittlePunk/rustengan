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
    node_ids: Vec<String>,
    storage: LinKv,
    tx: Sender<Event<Payload>>,
}

impl KafkaNode {
    fn send_proxy_node(
        &mut self,
        payload: Payload,
        idx: usize,
        output: &mut StdoutLock,
        rx: &Receiver<Event<Payload>>,
    ) -> Result<Payload> {
        let dest = self.node_ids.get(idx).unwrap();
        let mut message =
            Message::kv_message(self.node_id.as_str(), dest, Some(&mut self.id), None);
        message.body.payload = payload;
        message.send(output)?;
        loop {
            let rt = Runtime {
                id: &mut self.id,
                node_id: &self.node_id,
                rx,
                writer: output,
                in_reply_to: None,
            };
            let Event::Message(input) = rt.rx.recv()? else {
                panic!("got injected event when there's no event injection");
            };
            let mut reply = input.reply(Some(rt.id));
            let storage = &mut self.storage;
            match input.body.payload {
                Payload::SendOk { offset } => {
                    return Ok(Payload::SendOk { offset });
                }
                //   Forward
                // A --------> B
                // A waiting
                //    Forward
                // B ---------> A
                // B waiting
                // A, B在等待过程中需要处理属于自己Forward Send 事件，然后返回结果，否则陷入死锁
                Payload::ForwardSend { key, msg } => {
                    let offset = storage.send(rt, key, msg)?;
                    let payload = Payload::SendOk { offset };
                    reply.body.payload = payload;
                    reply.send(output)?;
                    // 检查storage是否收到SendOk包，收到立刻发送这里loop接收，然后返回我们等到的offset。
                    while let Some(index) = storage
                        .stash_event
                        .iter()
                        .position(|c| matches!(&c.body.payload, &Payload::SendOk { .. }))
                    {
                        self.tx
                            .send(Event::Message(storage.stash_event.remove(index)))?;
                    }
                }
                Payload::Send { .. }
                | Payload::Poll { .. }
                | Payload::CommitOffsets { .. }
                | Payload::ListCommittedOffsets { .. } => {
                    storage.stash_event.push(input);
                }
                _ => {
                    return Err(GanError::Normal(
                        "should not exist invalid response".to_string(),
                    ))
                }
            }
        }
    }
}

impl Node<(), Payload> for KafkaNode {
    fn from_init(_: (), init: Init, tx: Sender<Event<Payload>>) -> Result<Self>
    where
        Self: Sized,
    {
        Ok(KafkaNode {
            id: 1,
            node_id: init.node_id,
            node_ids: init.node_ids,
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
        match reply.body.payload {
            // receive a forward message
            Payload::ForwardSend { key, msg } => {
                let offset = self.storage.send(rt, key, msg)?;
                reply.body.payload = Payload::SendOk { offset };
            }
            Payload::Send { key, msg } => {
                if let Some((k, nid)) = key
                    .parse::<u64>()
                    .ok()
                    .zip((&self.node_id[1..]).parse::<u64>().ok())
                {
                    let idx = k % self.node_ids.len() as u64;
                    if idx != nid {
                        reply.body.payload = self.send_proxy_node(
                            Payload::ForwardSend { key, msg },
                            idx as usize,
                            output,
                            rx,
                        )?;
                        reply.send(output)?;
                        for event in self.storage.stash_event.drain(..) {
                            self.tx.send(Event::Message(event))?;
                        }
                        return Ok(());
                    }
                }
                let offset = self.storage.send(rt, key, msg)?;
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
                let committed_offsets = self.storage.list_committed_offsets(rt, keys);
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
const BATCH_SIZE: u64 = 20;

trait EntriesExt {
    fn append(&mut self, offset: u64, value: u64);
    fn new_key(key: &str, offset: u64) -> Self;
}

impl EntriesExt for String {
    fn append(&mut self, offset: u64, value: u64) {
        if !self.is_empty() {
            self.push(',');
        }
        self.push_str(format!("{offset}:{value}").as_str());
    }

    fn new_key(key: &str, offset: u64) -> Self {
        let start = offset - offset % BATCH_SIZE;
        format!("{}_{}_{}-{}", PREFIX_ENTRY, key, start, start + BATCH_SIZE)
    }
}

impl LinKv {
    fn send(&mut self, mut rt: Runtime<Payload>, key: String, value: u64) -> Result<u64> {
        let latest_key = format!("{}_{}", PREFIX_LATEST, key);
        let offset = self
            .read(&mut rt, &latest_key)?
            .parse::<u64>()
            .map(|x| x + 1)
            .unwrap_or(0);
        self.write(&mut rt, latest_key, offset.to_string())?;
        // write batch entry
        let entry_key = String::new_key(key.as_str(), offset);
        let mut entries = self.read(&mut rt, &entry_key)?;
        entries.append(offset, value);
        self.write(&mut rt, entry_key, entries)?;
        Ok(offset)
    }

    fn read_segment(
        &mut self,
        rt: &mut Runtime<Payload>,
        ofs: u64,
        key: &str,
        key_offsets: &mut Vec<(u64, u64)>,
    ) -> Result<()> {
        let mut start = ofs - ofs % BATCH_SIZE;
        loop {
            let entry_key = String::new_key(&key, start);
            let entries = self.read(rt, &entry_key)?;
            if entries.is_empty() {
                break;
            }
            for entry in entries.split(',') {
                let Some((o, v)) = entry.split_once(':').and_then(|(o, v)| o.parse().ok().zip(v.parse().ok())) else {
                    continue;
                };
                if o >= ofs {
                    key_offsets.push((o, v));
                }
            }
            start += BATCH_SIZE;
        }
        Ok(())
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
            self.read_segment(&mut rt, ofs, &key, &mut key_offsets)?;
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
            let commit_key = format!("{}_{}", PREFIX_COMMIT, key);
            self.write(&mut rt, commit_key, ofs.to_string())?;
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
                    .ok()
                    .and_then(|c| c.parse().ok());
                Some(k).zip(offset)
            })
            .collect()
    }
}

impl KV for LinKv {
    type Value = String;
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
                    return Ok(Default::default());
                }
                Payload::Error { code, text } => {
                    rt.in_reply_to = input.body.id;
                    return Err(GanError::Rpc { code, text });
                }

                Payload::Send { .. }
                | Payload::Poll { .. }
                | Payload::SendOk { .. }
                | Payload::CommitOffsets { .. }
                | Payload::ListCommittedOffsets { .. }
                | Payload::ForwardSend { .. } => {
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
                | Payload::SendOk { .. }
                | Payload::CommitOffsets { .. }
                | Payload::ListCommittedOffsets { .. }
                | Payload::ForwardSend { .. } => {
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
                | Payload::SendOk { .. }
                | Payload::Poll { .. }
                | Payload::CommitOffsets { .. }
                | Payload::ListCommittedOffsets { .. }
                | Payload::ForwardSend { .. } => {
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

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Payload {
    Send {
        key: String,
        msg: u64,
    },
    ForwardSend {
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
        value: String,
    },
    Write {
        key: String,
        value: String,
    },
    WriteOk,
    Cas {
        key: String,
        from: String,
        to: String,
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
