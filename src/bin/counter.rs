use std::io::StdoutLock;
use std::sync::mpsc::Receiver;

use rand::Rng;
use serde::{Deserialize, Serialize};

use rustengan::*;

const GLOBAL_KEY: &str = "Counter";

fn main() -> Result<()> {
    main_loop::<_, CounterNode<SeqKv>, _, _>(())?;
    Ok(())
}

struct SeqKv {}

impl KV for SeqKv {
    type Value = u64;
    type Payload = Payload;
    fn read(&self, rt: &mut Runtime<'_, '_, Self::Payload>, key: &str) -> Result<Self::Value> {
        let payload = Payload::KvRead {
            key: key.to_string(),
        };
        let mut message = Message::kv_message(rt.node_id, "seq-kv", Some(rt.id), rt.in_reply_to);
        message.body.payload = payload;
        message.send(rt.writer)?;
        let Event::Message(input) = rt.rx.recv()? else {
            panic!("got injected event when there's no event injection");
        };
        rt.in_reply_to = input.body.id;
        match input.body.payload {
            Payload::ReadOk { value } => {
                return Ok(value);
            }
            Payload::Error { code, text } => {
                return Err(GanError::Rpc { code, text });
            }
            _ => Err(GanError::Normal("should not be other payload".to_string())),
        }
    }

    fn write(
        &self,
        rt: &mut Runtime<'_, '_, Self::Payload>,
        key: String,
        value: Self::Value,
    ) -> Result<()> {
        let payload = Payload::Write { key, value };
        let mut message = Message::kv_message(rt.node_id, "seq-kv", Some(rt.id), rt.in_reply_to);
        message.body.payload = payload;
        message.send(rt.writer)?;
        let Event::Message(input) = rt.rx.recv()? else {
            panic!("got injected event when there's no event injection");
        };
        rt.in_reply_to = input.body.id;
        match input.body.payload {
            Payload::WriteOk => Ok(()),
            Payload::Error { code, text } => Err(GanError::Rpc { code, text }),
            _ => Err(GanError::Normal("should not be other payload".to_string())),
        }
    }

    fn compare_exchange(
        &self,
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
        let mut message = Message::kv_message(rt.node_id, "seq-kv", Some(rt.id), rt.in_reply_to);
        message.body.payload = payload;
        message.send(rt.writer)?;
        let Event::Message(input) = rt.rx.recv()? else {
            panic!("got injected event when there's no event injection");
        };
        rt.in_reply_to = input.body.id;
        match input.body.payload {
            Payload::CasOk => Ok(()),
            Payload::Error { code, text } => {
                // The requested operation expected some conditions to hold, and those conditions were not met.
                if code == 22 {
                    return Err(GanError::PreconditionFailed);
                }
                return Err(GanError::Rpc { code, text });
            }
            _ => Err(GanError::Normal("should not be other payload".to_string())),
        }
    }
}

struct CounterNode<K: KV> {
    id: usize,
    node_id: String,
    kv: K,
}
impl Node<(), Payload> for CounterNode<SeqKv> {
    fn from_init(_: (), init: Init, _: std::sync::mpsc::Sender<Event<Payload>>) -> Result<Self>
    where
        Self: Sized,
    {
        Ok(Self {
            id: 1,
            node_id: init.node_id,
            kv: SeqKv {},
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
        let rt = Runtime {
            id: &mut self.id,
            node_id: &self.node_id,
            rx,
            writer: output,
            in_reply_to: None,
        };
        match input.body.payload {
            Payload::Add { delta } => {
                add_delta(&mut self.kv, delta, rt)?;
                let mut reply = input.into_reply(Some(&mut self.id));
                reply.body.payload = Payload::AddOk;
                reply.send(output)?;
            }
            Payload::Read => {
                let value = read(&mut self.kv, rt)?;
                let mut reply = input.into_reply(Some(&mut self.id));
                reply.body.payload = Payload::ReadOk { value };
                reply.send(output)?;
            }
            Payload::CasOk
            | Payload::WriteOk
            | Payload::Error { .. }
            | Payload::ReadOk { .. }
            | Payload::Write { .. }
            | Payload::Cas { .. }
            | Payload::AddOk
            | Payload::KvRead { .. } => {
                return Err(GanError::Normal(
                    "we should never receive generate_ok".to_string(),
                ))
            }
        }
        Ok(())
    }
}

fn add_delta(kv: &mut SeqKv, delta: u64, rt: Runtime<Payload>) -> Result<()> {
    if delta == 0 {
        return Ok(());
    }
    let mut rt = rt;
    loop {
        let (old, new_rt) = read_inner(kv, rt)?;
        rt = new_rt;
        match kv.compare_exchange(&mut rt, GLOBAL_KEY, old, old + delta, true) {
            Ok(_) => return Ok(()),
            Err(GanError::PreconditionFailed) => (),
            Err(e) => return Err(e),
        }
    }
}

fn read(kv: &mut SeqKv, mut rt: Runtime<Payload>) -> Result<u64> {
    // Do a "sync" to read latest values. See https://github.com/jepsen-io/maelstrom/issues/39#issuecomment-1445414521
    // Looks like seq-kv is sequential across all keys.
    let mut rng = rand::thread_rng();
    kv.write(&mut rt, "sync".to_string(), rng.gen_range(0..1000_000_000))?;
    Ok(read_inner(kv, rt)?.0)
}

fn read_inner<'a, 'stdout>(
    kv: &mut SeqKv,
    mut rt: Runtime<'a, 'stdout, Payload>,
) -> Result<(u64, Runtime<'a, 'stdout, Payload>)> {
    match kv.read(&mut rt, GLOBAL_KEY) {
        Ok(g) => Ok((g, rt)),
        // key not exist
        Err(GanError::Rpc { code, .. }) if code == 20 => {
            let _ = kv.write(&mut rt, GLOBAL_KEY.to_string(), 0)?;
            return Ok((0, rt));
        }
        Err(e) => Err(e),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Add {
        delta: u64,
    },
    AddOk,
    #[default]
    Read,
    ReadOk {
        value: u64,
    },
    #[serde(rename = "read")]
    KvRead {
        key: String,
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
    Error {
        code: u8,
        text: String,
    },
}
