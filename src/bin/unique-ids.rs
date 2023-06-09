use std::io::StdoutLock;
use std::sync::mpsc::Receiver;

use serde::{Deserialize, Serialize};

use rustengan::*;

fn main() -> Result<()> {
    main_loop::<_, UniqueNode, _, _>(())?;
    Ok(())
}

struct UniqueNode {
    id: usize,
    node_id: String,
}
impl Node<(), Payload> for UniqueNode {
    fn from_init(_: (), init: Init, _: std::sync::mpsc::Sender<Event<Payload>>) -> Result<Self>
    where
        Self: Sized,
    {
        Ok(UniqueNode {
            id: 1,
            node_id: init.node_id,
        })
    }
    fn step(
        &mut self,
        input: Event<Payload>,
        output: &mut StdoutLock,
        _: &Receiver<Event<Payload>>,
    ) -> Result<()> {
        let Event::Message(input) = input else {
            panic!("got injected event when there's no event injection");
        };
        let mut reply = input.into_reply(Some(&mut self.id));
        match reply.body.payload {
            Payload::Generate => {
                let guid = format!("{}-{}", self.node_id, self.id);
                reply.body.payload = Payload::GenerateOk { guid };
                reply.send(output)?;
            }
            Payload::GenerateOk { .. } => {
                return Err(GanError::Normal(
                    "we should never receive generate_ok".to_string(),
                ))
            }
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
