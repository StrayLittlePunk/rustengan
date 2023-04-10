use std::io::{BufRead, StdoutLock, Write};

use anyhow::Context;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

pub fn main_loop<S, N, P, I>(init_state: S) -> anyhow::Result<()>
where
    P: DeserializeOwned + Serialize + Send + 'static,
    N: Node<S, P, I>,
    S: Send,
    I: Send + 'static,
{
    let stdin = std::io::stdin().lock();
    let mut stdin = stdin.lines();
    let mut stdout = std::io::stdout().lock();

    let init_msg: Message<InitPayload> = serde_json::from_str(
        stdin
            .next()
            .expect("no init message received")
            .context("failed to read from stdin")?
            .as_str(),
    )
    .context("init message could not be deserialized")?;
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
    serde_json::to_writer(&mut stdout, &reply).context("serialize response to init")?;
    stdout.write_all(b"\n").context("write \\n failed")?;

    let (tx, rx) = std::sync::mpsc::channel();
    let stdin_tx = tx.clone();
    drop(stdin);
    let mut node = N::from_init(init_state, init, tx).expect("should construct a node");
    let handle = std::thread::spawn(move || {
        let stdin = std::io::stdin().lock();
        for input in stdin.lines() {
            let input: Message<P> = serde_json::from_str(
                &input.context("Maelstrom input from STDIN could not be deserialized")?,
            )?;

            if let Err(_) = stdin_tx.send(Event::Message(input)) {
                return Ok::<_, anyhow::Error>(());
            }
        }
        let _ = stdin_tx.send(Event::EOF);
        Ok(())
    });
    for input in rx {
        node.step(input, &mut stdout)
            .context("Node step function failed")?;
    }

    handle
        .join()
        .expect("stdin thread panicked")
        .context("stdin return error")?;
    Ok(())
}

pub trait Node<S, Payload, InjectedPayload = ()> {
    fn from_init(
        init_state: S,
        init: Init,
        injecter: std::sync::mpsc::Sender<Event<Payload, InjectedPayload>>,
    ) -> anyhow::Result<Self>
    where
        Self: Sized;

    fn step(
        &mut self,
        input: Event<Payload, InjectedPayload>,
        output: &mut StdoutLock,
    ) -> anyhow::Result<()>;
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

    pub fn send(&self, output: &mut impl Write) -> anyhow::Result<()>
    where
        Self: Serialize,
    {
        serde_json::to_writer(&mut *output, self)?;
        output.write_all(b"\n").context("write trailing newline")?;
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
