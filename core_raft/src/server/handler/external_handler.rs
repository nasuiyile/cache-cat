use crate::network::model::Request;
use crate::network::node::{App, TypeConfig, get_app, get_group};
use crate::server::handler::model::*;
use async_trait::async_trait;
use bytes::Bytes;
use openraft::Snapshot;
use openraft::raft::{
    AppendEntriesResponse, ClientWriteResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    SnapshotResponse, VoteRequest, VoteResponse,
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::io::Cursor;
use std::time::Instant;

pub type HandlerEntry = (u32, fn() -> Box<dyn RpcHandler>);

pub static HANDLER_TABLE: &[HandlerEntry] = &[
    (1, || Box::new(RpcMethod { func: print_test })),
    (2, || Box::new(RpcMethod { func: write })),
    (3, || Box::new(RpcMethod { func: read })),
    (6, || Box::new(RpcMethod { func: vote })),
    (7, || {
        Box::new(RpcMethod {
            func: append_entries,
        })
    }),
    (8, || {
        Box::new(RpcMethod {
            func: install_full_snapshot,
        })
    }),
];
fn hash_string(s: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    s.hash(&mut hasher);
    hasher.finish()
}

#[async_trait]
pub trait RpcHandler: Send + Sync {
    // 将 app 改为 Arc 传递，更符合异步环境下的生命周期要求
    async fn call(&self, app: App, data: Bytes) -> Bytes;
}

// 修改函数指针定义，使其支持异步返回 Future
// 这里使用泛型 F 来适配异步函数
pub struct RpcMethod<Req, Res, Fut>
where
    Fut: Future<Output = Res> + Send,
{
    // 注意：Rust 的纯函数指针 fn 不能直接是 async 的
    // 我们这里让 func 返回一个 Future
    func: fn(App, Req) -> Fut,
}

#[async_trait]
impl<Req, Res, Fut> RpcHandler for RpcMethod<Req, Res, Fut>
where
    Req: Send + 'static + DeserializeOwned,
    Res: Send + 'static + Serialize,
    Fut: Future<Output = Res> + Send + 'static,
{
    async fn call(&self, app: App, data: Bytes) -> Bytes {
        // 反序列化
        let req: Req = bincode2::deserialize(data.as_ref()).expect("Failed to deserialize");
        // 执行异步业务函数
        let res = (self.func)(app, req).await;
        // 序列化
        let encoded: Vec<u8> = bincode2::serialize(&res).expect("Failed to serialize");
        encoded.into()
    }
}

// --- 业务函数全部改为 async ---

async fn print_test(_app: App, d: PrintTestReq) -> PrintTestRes {
    PrintTestRes { message: d.message }
}

// 主节点才能成功调用这个方法，其他节点会失败
async fn write(app: App, req: Request) -> ClientWriteResponse<TypeConfig> {
    // 根据请求判断属于哪个组ftokio::spawnz
    let group = get_group(&app, req.hash_code());
    let res: ClientWriteResponse<TypeConfig> = group
        .raft
        .client_write(req)
        .await
        .expect("Raft write failed");
    res
}
async fn read(app: App, req: String) -> Option<String> {
    // let group = get_group(&app, hash_string(&req));
    // let kvs = group.state_machine.data.kvs.lock().await;
    // let value = kvs.get(&req);
    // value.map(|v| String::from_utf8(v.tostring()))
    todo!()
}

async fn vote(app: App, req: VoteReq) -> VoteResponse<TypeConfig> {
    // openraft 的 vote 是异步的
    let group = get_app(&app, req.group_id);
    group.raft.vote(req.vote).await.expect("Raft vote failed")
}

//理论上只有从节点会被调用这个方法
async fn append_entries(app: App, req: AppendEntriesReq) -> AppendEntriesResponse<TypeConfig> {
    let start = Instant::now();
    let e = req.append_entries.entries.is_empty();
    let res = get_app(&app, req.group_id)
        .raft
        .append_entries(req.append_entries)
        .await
        .expect("Raft append_entries failed");
    let elapsed = start.elapsed();
    if !e {
        tracing::info!("append 从节点内部处理: {:?} ", elapsed);
    }
    res
}

//InstallFullSnapshotReq 把openraft自带的俩个参数包裹在一起了
async fn install_full_snapshot(
    app: App,
    req: InstallFullSnapshotReq,
) -> SnapshotResponse<TypeConfig> {
    let snapshot = Snapshot {
        meta: req.snapshot_meta,
        snapshot: Cursor::new(req.snapshot),
    };
    get_app(&app, req.group_id)
        .raft
        .install_full_snapshot(req.vote, snapshot)
        .await
        .expect("Raft install_snapshot failed")
}
