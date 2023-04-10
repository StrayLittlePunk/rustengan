use std::io::StdoutLock;

use anyhow::Context;
use serde::{Deserialize, Serialize};

use rustengan::*;

fn main() -> anyhow::Result<()> {
    main_loop::<_, EchoNode, _, _>(())?;
    Ok(())
}

struct EchoNode {
    id: usize,
}
impl Node<(), Payload> for EchoNode {
    fn from_init(_: (), _: Init, _: std::sync::mpsc::Sender<Event<Payload>>) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(EchoNode { id: 1 })
    }
    fn step(&mut self, input: Event<Payload>, output: &mut StdoutLock) -> anyhow::Result<()> {
        let Event::Message(input) = input else {
            panic!("got injected event when there's no event injection");
        };
        let mut reply = input.into_reply(Some(&mut self.id));
        match reply.body.payload {
            Payload::Echo { echo } => {
                reply.body.payload = Payload::EchoOk { echo };
                reply.send(output).context("serialze repsonse to echo")?;
            }
            Payload::EchoOk { .. } => {}
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Echo { echo: String },
    EchoOk { echo: String },
}
