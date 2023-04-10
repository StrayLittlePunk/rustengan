use std::io::{StdoutLock, Write};

use anyhow::Context;
use serde::{Deserialize, Serialize};

use rustengan::*;

fn main() -> anyhow::Result<()> {
    main_loop::<_, EchoNode, _>(())?;
    Ok(())
}

struct EchoNode {
    id: usize,
}
impl Node<(), Payload> for EchoNode {
    fn from_init(_: (), _: Init) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(EchoNode { id: 1 })
    }
    fn step(&mut self, input: Message<Payload>, output: &mut StdoutLock) -> anyhow::Result<()> {
        match input.body.payload {
            Payload::Echo { echo } => {
                let reply = Message {
                    src: input.dst,
                    dst: input.src,
                    body: Body {
                        id: Some(self.id),
                        in_reply_to: input.body.id,
                        payload: Payload::EchoOk { echo },
                    },
                };
                serde_json::to_writer(&mut *output, &reply).context("serialze repsonse to echo")?;
                output.write_all(b"\n").context("write \\n failed")?;
                self.id += 1;
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
