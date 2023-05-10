use std::io::{BufRead, StdoutLock, Write};
use std::sync::mpsc::Receiver;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub type Result<T> = std::result::Result<T, GanError>;

pub fn main_loop<S, N, P, I>(init_state: S) -> Result<()>
where
    P: DeserializeOwned + Serialize + Send + 'static,
    N: Node<S, P, I>,
    S: Send,
    I: Send + 'static,
{
    let stdin = std::io::stdin().lock();
    let mut stdin = stdin.lines();
    let mut stdout = std::io::stdout().lock();

    let init_msg: Message<InitPayload> =
        serde_json::from_str(&stdin.next().expect("no init message received")?)?;
    let InitPayload::Init(init) = init_msg.body.payload else {
        panic!("first message should be init");
    };
    let reply = Message {
        src: init_msg.dst,
        dst: init_msg.src,
        body: Body {
            id: Some(0),
            in_reply_to: init_msg.body.id,
            payload: InitPayload::InitOk,
        },
    };
    serde_json::to_writer(&mut stdout, &reply)?;
    stdout.write_all(b"\n")?;

    let (tx, rx) = std::sync::mpsc::channel();
    let stdin_tx = tx.clone();
    drop(stdin);
    let mut node = N::from_init(init_state, init, tx).expect("should construct a node");
    let handle = std::thread::spawn(move || {
        let stdin = std::io::stdin().lock();
        for input in stdin.lines() {
            let input: Message<P> = serde_json::from_str(&input?)?;
            if let Err(_) = stdin_tx.send(Event::Message(input)) {
                return Ok::<_, GanError>(());
            }
        }
        let _ = stdin_tx.send(Event::EOF);
        Ok(())
    });
    loop {
        let Ok(input) = rx.recv() else {
            break;
        };
        node.step(input, &mut stdout, &rx)?;
    }
    let _ = handle.join().expect("stdin thread panicked");
    Ok(())
}

pub trait Node<S, Payload, InjectedPayload = ()> {
    fn from_init(
        init_state: S,
        init: Init,
        injecter: std::sync::mpsc::Sender<Event<Payload, InjectedPayload>>,
    ) -> Result<Self>
    where
        Self: Sized;

    fn step(
        &mut self,
        input: Event<Payload, InjectedPayload>,
        output: &mut StdoutLock,
        rx: &Receiver<Event<Payload, InjectedPayload>>,
    ) -> Result<()>;
}

pub struct Runtime<'s, 'stdout, Payload> {
    pub id: &'s mut usize,
    pub node_id: &'s str,
    pub rx: &'s Receiver<Event<Payload>>,
    pub writer: &'s mut StdoutLock<'stdout>,
    pub in_reply_to: Option<usize>,
}

pub trait KV {
    type Value;
    type Payload;
    fn read(&self, rt: &mut Runtime<'_, '_, Self::Payload>, key: &str) -> Result<Self::Value>;
    fn write(
        &self,
        rt: &mut Runtime<'_, '_, Self::Payload>,
        key: String,
        value: Self::Value,
    ) -> Result<()>;
    fn compare_exchange(
        &self,
        rt: &mut Runtime<'_, '_, Self::Payload>,
        key: &str,
        old: Self::Value,
        new: Self::Value,
        create_if_not_exists: bool,
    ) -> Result<()>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message<Payload> {
    pub src: String,
    #[serde(rename = "dest")]
    pub dst: String,
    pub body: Body<Payload>,
}

impl<Payload> Message<Payload> {
    pub fn into_reply(self, id: Option<&mut usize>) -> Self {
        Self {
            src: self.dst,
            dst: self.src,
            body: Body {
                id: id.map(|id| {
                    let mid = *id;
                    *id += 1;
                    mid
                }),
                in_reply_to: self.body.id,
                payload: self.body.payload,
            },
        }
    }

    pub fn kv_message(
        node_id: &str,
        typ: &str,
        id: Option<&mut usize>,
        in_reply_to: Option<usize>,
    ) -> Self
    where
        Payload: Default,
    {
        Message {
            src: node_id.to_string(),
            dst: typ.to_string(),
            body: Body {
                id: id.map(|id| {
                    let mid = *id;
                    *id += 1;
                    mid
                }),
                in_reply_to,
                payload: Default::default(),
            },
        }
    }

    pub fn send(&self, output: &mut impl Write) -> Result<()>
    where
        Self: Serialize,
    {
        serde_json::to_writer(&mut *output, self)?;
        output.write_all(b"\n")?;
        Ok(())
    }
}
#[derive(Debug, Clone)]
pub enum Event<Payload, InjectedPayload = ()> {
    Message(Message<Payload>),
    Injected(InjectedPayload),
    EOF,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Body<Payload> {
    #[serde(rename = "msg_id")]
    pub id: Option<usize>,
    pub in_reply_to: Option<usize>,
    #[serde(flatten)]
    pub payload: Payload,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Init {
    pub node_id: String,
    pub node_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum InitPayload {
    Init(Init),
    InitOk,
}

#[derive(Error, Debug)]
pub enum GanError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("{0}")]
    SendError(String),
    #[error(transparent)]
    RecvError(#[from] std::sync::mpsc::RecvError),
    #[error("receive rpc error with code({code}), text: {text}")]
    Rpc { code: u8, text: String },
    #[error("{0}")]
    Normal(String),
    #[error("cas precondition failed")]
    PreconditionFailed,
}

impl<T> From<std::sync::mpsc::SendError<T>> for GanError {
    fn from(value: std::sync::mpsc::SendError<T>) -> Self {
        Self::SendError(format!("{}", value))
    }
}
