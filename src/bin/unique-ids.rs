use std::io::{StdoutLock, Write};

use anyhow::{bail, Context};
use serde::{Deserialize, Serialize};

use rustengan::*;

fn main() -> anyhow::Result<()> {
    main_loop::<_, UniqueNode, _>(())?;
    Ok(())
}

struct UniqueNode {
    id: usize,
    node_id: String,
}
impl Node<(), Payload> for UniqueNode {
    fn from_init(_: (), init: Init) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(UniqueNode {
            id: 1,
            node_id: init.node_id,
        })
    }
    fn step(&mut self, input: Message<Payload>, output: &mut StdoutLock) -> anyhow::Result<()> {
        let mut reply = input.into_reply(Some(&mut self.id));
        match reply.body.payload {
            Payload::Generate => {
                let guid = format!("{}-{}", self.node_id, self.id);
                reply.body.payload = Payload::GenerateOk { guid };
                serde_json::to_writer(&mut *output, &reply)
                    .context("serialze repsonse to generate")?;
                output.write_all(b"\n").context("write \\n failed")?;
            }
            Payload::GenerateOk { .. } => bail!("we should never receive generate_ok"),
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Generate,
    GenerateOk {
        #[serde(rename = "id")]
        guid: String,
    },
}
