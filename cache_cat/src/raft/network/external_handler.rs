use crate::raft::network::model::{
    AddNodeReq, AppendEntriesReq, GetReq, GetRes, InstallFullSnapshotReq, PrintTestReq,
    PrintTestRes, VoteReq,
};
use crate::raft::types::core::moka::MyValue;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::membership::JoinRequest;
use crate::raft::types::entry::request::Request;
use crate::raft::types::raft_types::{App, Node, NodeId, TypeConfig, get_app, get_group_by_key};
use async_trait::async_trait;
use bytes::Bytes;
use openraft::ReadPolicy::LeaseRead;
use openraft::async_runtime::WatchReceiver;
use openraft::error::RaftError;
use openraft::raft::{AppendEntriesResponse, ClientWriteResponse, SnapshotResponse, VoteResponse};
use openraft::{ChangeMembers, Snapshot};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::collections::{BTreeMap, BTreeSet};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;
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
    (9, || Box::new(RpcMethod { func: add_node })),
];
fn hash_string(s: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    s.hash(&mut hasher);
    hasher.finish()
}

#[async_trait]
pub trait RpcHandler: Send + Sync {
    // 将 app 改为 Arc 传递，更符合异步环境下的生命周期要求
    async fn internal_call(&self, app: App, data: Bytes) -> Bytes;
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
    async fn internal_call(&self, app: App, data: Bytes) -> Bytes {
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
async fn print_test(_app: App, d: PrintTestReq) -> Result<PrintTestRes, String> {
    // sleep(std::time::Duration::from_secs(10));
    Ok(PrintTestRes { message: d.message })
}

// 主节点才能成功调用这个方法，其他节点会失败
async fn write(app: App, req: Request) -> Result<ClientWriteResponse<TypeConfig>, String> {
    let group = get_app(&app, req.get_group_id());
    group.raft.client_write(req).await.map_err(|e| {
        tracing::error!("write error: {:?}", e);
        e.to_string()
    })
}
async fn read(app: App, get_req: GetReq) -> Result<GetRes, String> {
    let group = get_group_by_key(&app, &get_req.key);
    let ret = group.raft.get_read_linearizer(LeaseRead).await;

    let value = match ret {
        Ok(linearizer) => {
            linearizer.await_ready(&group.raft).await.unwrap();
            group.state_machine.data.kvs.cache.get(&get_req.key).await
        }
        Err(e) => return Err(e.to_string()),
    };
    match value {
        None => Ok(GetRes { value: None }),
        Some(v) => match v.data {
            ValueObject::String(value) => Ok(GetRes { value: Some(value) }),
            _ => Err("value is not string".to_string()),
        },
    }
}

async fn vote(app: App, req: VoteReq) -> Result<VoteResponse<TypeConfig>, String> {
    // openraft 的 vote 是异步的
    let group = get_app(&app, req.group_id);
    group.raft.vote(req.vote).await.map_err(|e| {
        tracing::error!("vote error: {:?}", e);
        e.to_string()
    })
}

//理论上只有从节点会被调用这个方法
async fn append_entries(
    app: App,
    req: AppendEntriesReq,
) -> Result<AppendEntriesResponse<TypeConfig>, String> {
    let res = get_app(&app, req.group_id)
        .raft
        .append_entries(req.append_entries)
        .await
        .map_err(|e| e.to_string());
    res
}

// 从节点收到数据 在这里序列化到磁盘 后续install_full_snapshot会从磁盘中反序列化
async fn install_full_snapshot(
    app: App,
    req: InstallFullSnapshotReq,
) -> Result<SnapshotResponse<TypeConfig>, String> {
    let snapshot = Snapshot {
        meta: req.snapshot_meta,
        snapshot: req.snapshot,
    };
    get_app(&app, req.group_id)
        .raft
        .install_full_snapshot(req.vote, snapshot)
        .await
        .map_err(|e| e.to_string())
}

async fn add_node(app: App, req: JoinRequest) -> Result<(), String> {
    for app in app.iter() {
        let node = Node {
            node_id: req.node_id,
            endpoint: req.endpoint.clone(),
        };
        // 已经存在就不继续加入
        let existed = app.raft.voter_ids().any(|id| id == node.node_id);
        if existed {
            continue;
        }
        let _ = app.raft.add_learner(node.node_id, node.clone(), true).await;
        // 使用 AddVoters 而不是传入完整集合
        // 这会自动计算并添加到现有成员中
        let mut map = BTreeMap::new();
        map.insert(node.node_id, node.clone());
        let changes = ChangeMembers::AddVoters(map);
        app.raft
            .change_membership(changes, true)
            .await
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}
