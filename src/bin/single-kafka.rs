use std::collections::{HashMap, VecDeque};
use std::io::StdoutLock;
use std::marker::PhantomData;
use std::sync::mpsc::Receiver;

use serde::{Deserialize, Serialize};

use rustengan::*;

fn main() -> Result<()> {
    main_loop::<_, KafkaNode<String, u64>, _, _>(())?;
    Ok(())
}

struct KafkaNode<K, V> {
    id: usize,
    #[allow(unused)]
    node_id: String,
    storage: KafkaStorage<K, V>,
}

impl Node<(), Payload> for KafkaNode<String, u64> {
    fn from_init(_: (), init: Init, _: std::sync::mpsc::Sender<Event<Payload>>) -> Result<Self>
    where
        Self: Sized,
    {
        Ok(KafkaNode {
            id: 1,
            node_id: init.node_id,
            storage: KafkaStorage {
                data_block: Default::default(),
                topic_offsets: Default::default(),
                topic_committed_offsets: Default::default(),
                current_offset: 0,
                _mark: PhantomData,
            },
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
            Payload::Send { key, msg } => {
                let offset = self.storage.send(key, msg)?;
                reply.body.payload = Payload::SendOk { offset };
            }
            Payload::Poll { offsets } => {
                let msgs = self.storage.poll(offsets)?;
                reply.body.payload = Payload::PollOk { msgs };
            }
            Payload::CommitOffsets { offsets } => {
                self.storage.commit_offsets(offsets)?;
                reply.body.payload = Payload::CommitOffsetsOk;
            }
            Payload::ListCommittedOffsets { keys } => {
                let committed_offsets = self.storage.list_committed_offsets(keys);
                reply.body.payload = Payload::ListCommittedOffsetsOk {
                    offsets: committed_offsets,
                };
            }
            Payload::Error { code, text } => {
                eprintln!("kafka node step call error({code}): {text}");
                return Ok(());
            }
            Payload::SendOk { .. }
            | Payload::CommitOffsetsOk
            | Payload::ListCommittedOffsetsOk { .. }
            | Payload::PollOk { .. } => {
                return Err(GanError::Normal(
                    "should not exist invalid response".to_string(),
                ));
            }
        }
        reply.send(output)?;
        Ok(())
    }
}

struct KafkaStorage<K, V> {
    // storage, future maybe in disk, shard, partition
    data_block: Vec<u8>,
    topic_offsets: HashMap<K, VecDeque<u64>>,
    topic_committed_offsets: HashMap<K, u64>,
    current_offset: u64,
    _mark: PhantomData<V>,
}

// Record format
//    u32         u64                V
// +--------+-------------+-----------------------------+
// | length |   offset    |       value                 |
// +--------+-------------+-----------------------------+
struct Record<V> {
    offset: u64,
    value: V,
}

const U32_LEN: usize = std::mem::size_of::<u32>();
const U64_LEN: usize = std::mem::size_of::<u64>();

impl<K, V> KafkaStorage<K, V>
where
    K: Clone + IntoBytes + Eq + std::hash::Hash,
    V: IntoBytes + FromBytes,
{
    // 以为commit_offset会清除掉offset之前的data，降低存储大小，原来题意是想commit_offset只是更新下committed offset，在list_commit_offset使用吐出去。
    // 这个删除api看将来是否用得到
    #[allow(dead_code)]
    fn remove_data(&mut self, offsets: HashMap<K, u64>) -> Result<()> {
        if offsets.is_empty() {
            return Ok(());
        }
        for (k, offset) in offsets.into_iter() {
            if let Some(queue) = self.topic_offsets.get_mut(&k) {
                while let Some(ofs) = queue.pop_front() {
                    if ofs > offset {
                        queue.push_front(ofs);
                        break;
                    }
                    Self::remove_record(&mut self.data_block, ofs)?;
                }
                if queue.is_empty() {
                    self.topic_offsets.remove(&k);
                }
            }
        }
        self.data_block.shrink_to_fit();
        Ok(())
    }

    fn send(&mut self, key: K, value: V) -> Result<u64> {
        let offset = self.current_offset;
        let record = Record { offset, value };
        let r = record.to_le_bytes();
        let record_length = r.len() + U32_LEN;
        self.data_block
            .extend_from_slice((record_length as u32).to_le_bytes().as_slice());
        self.data_block.extend_from_slice(r.as_slice());
        let queue = self.topic_offsets.entry(key).or_insert(Default::default());
        queue.push_back(offset);
        self.current_offset += record_length as u64;
        Ok(offset)
    }

    fn poll(&mut self, offsets: HashMap<K, u64>) -> Result<HashMap<K, Vec<(u64, V)>>> {
        let mut result = HashMap::new();
        // 没数据或者传入空offsets都直接返回
        if self.data_block.len() <= U32_LEN || offsets.is_empty() {
            return Ok(result);
        }
        for (k, offset) in offsets.into_iter() {
            if let Some(queue) = self.topic_offsets.get_mut(&k) {
                // 找出大于等于offset的所有offset
                let index = queue.binary_search(&offset).unwrap_or_else(|near| near);
                // 没找到而且都比队列offset的大，跳过
                if index >= queue.len() {
                    continue;
                }
                let offset_slice = queue.make_contiguous();
                let Some(values) = Self::parse_records(&self.data_block, &offset_slice[index..]) else {
                    return Err(GanError::Normal("解析record时候根据offset没找到, 本应该一定有的".to_string()));
                };
                result.entry(k).or_insert(values);
            }
        }
        Ok(result)
    }

    fn commit_offsets(&mut self, offsets: HashMap<K, u64>) -> Result<()> {
        if offsets.is_empty() {
            return Ok(());
        }
        for (k, offset) in offsets.into_iter() {
            self.topic_committed_offsets.insert(k, offset);
        }
        Ok(())
    }

    fn list_committed_offsets(&mut self, keys: Vec<K>) -> HashMap<K, u64> {
        keys.into_iter()
            .filter_map(|k| {
                let offset = self.topic_committed_offsets.get(&k).map(|&o| o);
                Some(k).zip(offset)
            })
            .collect()
    }

    // TODO: 优化删除记录，现在remove字节，然后收紧，效率O(n^2)
    fn remove_record(data_block: &mut Vec<u8>, offset: u64) -> Result<()> {
        let mut datas = data_block.as_slice();
        let mut idx = 0;
        loop {
            let Some((data, length)) =  to_u32(datas) else {
                break;
            };
            let Some((data, ofs)) = to_u64(data) else {
                break;
            };
            let value_length = length as usize - U32_LEN - U64_LEN;
            if data.len() < value_length {
                break;
            }
            if ofs != offset {
                datas = &data[value_length..];
                idx += length as usize;
                continue;
            }
            let _ = data_block.drain(idx..idx + length as usize);
            return Ok(());
        }

        Err(GanError::Normal("根据offset没找到对应数据块".to_string()))
    }

    fn parse_records(mut data_block: &[u8], offsets: &[u64]) -> Option<Vec<(u64, V)>> {
        if offsets.is_empty() {
            return None;
        }
        let mut result = Vec::new();
        loop {
            let Some((data, length)) =  to_u32(data_block) else {
                break;
            };
            let Some((data, ofs)) = to_u64(data) else {
                break;
            };
            let value_length = length as usize - U32_LEN - U64_LEN;
            if data.len() < value_length {
                break;
            }
            let offset = offsets[result.len()];
            if ofs != offset {
                data_block = &data[value_length..];
                continue;
            }
            let value = V::from_bytes(&data[..value_length]);
            data_block = &data[value_length..];
            result.push((offset, value));
            // 已经找完成就返回
            if offsets.len() == result.len() {
                return Some(result);
            }
        }
        //TODO: 要判断offset长度跟results长度一样，一样才拿全了，否则要么offsets又问题，要么datablock又问题，应该返回result，
        // 不过现在主要测试分布式系统日志存储，所以后期优化。
        if result.is_empty() {
            None
        } else {
            Some(result)
        }
    }
}

fn to_u32(mut data: &[u8]) -> Option<(&[u8], u32)> {
    if data.len() <= U32_LEN {
        return None;
    }
    let length = u32::from_le_bytes((&data[..U32_LEN]).try_into().unwrap());
    data = &data[U32_LEN..];
    Some((data, length))
}

fn to_u64(mut data: &[u8]) -> Option<(&[u8], u64)> {
    if data.len() <= U64_LEN {
        return None;
    }
    let length = u64::from_le_bytes((&data[..U64_LEN]).try_into().unwrap());
    data = &data[U64_LEN..];
    Some((data, length))
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
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
}

trait IntoBytes: Sized {
    type Output: AsSlice;
    fn to_le_bytes(self) -> Self::Output;
}

macro_rules! impl_into_bytes {
    ($($t:ty),*) => {
        $(
             impl IntoBytes for $t {
                 type Output = [u8; std::mem::size_of::<Self>()];
                 fn to_le_bytes(self) -> Self::Output {
                     self.to_le_bytes()
                 }
             }
        )*
    };
}

impl_into_bytes!(u32, u64);

impl<V: IntoBytes> IntoBytes for Record<V> {
    type Output = Vec<u8>;
    fn to_le_bytes(self) -> Self::Output {
        let mut result = Vec::new();
        result.extend_from_slice(self.offset.to_le_bytes().as_slice());
        result.extend_from_slice(self.value.to_le_bytes().as_slice());
        result
    }
}

impl IntoBytes for String {
    type Output = Vec<u8>;
    fn to_le_bytes(self) -> Self::Output {
        self.into_bytes()
    }
}

trait FromBytes: Sized {
    fn from_bytes(slice: &[u8]) -> Self;
}

impl FromBytes for u64 {
    fn from_bytes(slice: &[u8]) -> Self {
        u64::from_le_bytes((&slice[..U64_LEN]).try_into().unwrap())
    }
}

trait AsSlice {
    fn as_slice(&self) -> &[u8];
}

impl<const N: usize> AsSlice for [u8; N] {
    fn as_slice(&self) -> &[u8] {
        self.as_slice()
    }
}

impl AsSlice for Vec<u8> {
    fn as_slice(&self) -> &[u8] {
        self.as_slice()
    }
}
