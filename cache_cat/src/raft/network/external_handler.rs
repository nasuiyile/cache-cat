use crate::raft::network::model::{
    AppendEntriesReq, GetReq, GetRes, InstallFullSnapshotReq, PrintTestReq, PrintTestRes, VoteReq,
};
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::membership::JoinRequest;
use crate::raft::types::entry::request::Request;
use crate::raft::types::raft_types::{
     CacheCatApp, Node, TypeConfig,
};
use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use openraft::ReadPolicy::LeaseRead;
use openraft::raft::{
    AppendEntriesResponse, ClientWriteResponse, SnapshotResponse, VoteResponse, WriteResult,
};
use openraft::{ChangeMembers, Snapshot};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::collections::BTreeMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;
use tracing::info;

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
    (10, || Box::new(RpcMethod { func: batch_write })),
];
fn hash_string(s: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    s.hash(&mut hasher);
    hasher.finish()
}

#[async_trait]
pub trait RpcHandler: Send + Sync {
    // 将 app 改为 Arc 传递，更符合异步环境下的生命周期要求
    async fn internal_call(&self, app: Arc<CacheCatApp>, data: Bytes) -> Bytes;
}

// 修改函数指针定义，使其支持异步返回 Future
// 这里使用泛型 F 来适配异步函数
pub struct RpcMethod<Req, Res, Fut>
where
    Fut: Future<Output = Res> + Send,
{
    // 注意：Rust 的纯函数指针 fn 不能直接是 async 的
    // 我们这里让 func 返回一个 Future
    func: fn(Arc<CacheCatApp>, Req) -> Fut,
}

#[async_trait]
impl<Req, Res, Fut> RpcHandler for RpcMethod<Req, Res, Fut>
where
    Req: Send + 'static + DeserializeOwned,
    Res: Send + 'static + Serialize,
    Fut: Future<Output = Res> + Send + 'static,
{
    async fn internal_call(&self, app: Arc<CacheCatApp>, data: Bytes) -> Bytes {
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
async fn print_test(_app: Arc<CacheCatApp>, d: PrintTestReq) -> Result<PrintTestRes, String> {
    // sleep(std::time::Duration::from_secs(10));
    Ok(PrintTestRes { message: d.message })
}

// 主节点才能成功调用这个方法，其他节点会失败
pub async fn write(
    app: Arc<CacheCatApp>,
    req: Request,
) -> Result<ClientWriteResponse<TypeConfig>, String> {
    app.raft.client_write(req).await.map_err(|e| {
        tracing::error!("write error: {:?}", e);
        e.to_string()
    })
}

pub async fn batch_write(
    app: Arc<CacheCatApp>,
    req: Vec<Request>,
) -> Result<Vec<Result<WriteResult<TypeConfig>, String>>, String> {
    let stream = app.raft.client_write_many(req).await.map_err(|e| {
        tracing::error!("write error: {:?}", e);
        e.to_string()
    })?;

    // 映射错误类型并等待所有结果收集到 Vec 中
    let results: Vec<Result<WriteResult<TypeConfig>, String>> = stream
        .map(|res| res.map_err(|e| e.to_string()))
        .collect() // 这里会异步等待流结束
        .await;

    Ok(results)
}

async fn read(app: Arc<CacheCatApp>, get_req: GetReq) -> Result<GetRes, String> {
    let ret = app.raft.get_read_linearizer(LeaseRead).await;

    let value = match ret {
        Ok(linearizer) => {
            linearizer.await_ready(&app.raft).await.unwrap();
            app.state_machine.data.kvs.cache.get(&get_req.key).await
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

async fn vote(app: Arc<CacheCatApp>, req: VoteReq) -> Result<VoteResponse<TypeConfig>, String> {
    // openraft 的 vote 是异步的
    app.raft.vote(req.vote).await.map_err(|e| {
        tracing::error!("vote error: {:?}", e);
        e.to_string()
    })
}

//理论上只有从节点会被调用这个方法
async fn append_entries(
    app: Arc<CacheCatApp>,
    req: AppendEntriesReq,
) -> Result<AppendEntriesResponse<TypeConfig>, String> {
    let res = app
        .raft
        .append_entries(req.append_entries)
        .await
        .map_err(|e| e.to_string());
    res
}

// 从节点收到数据 在这里序列化到磁盘 后续install_full_snapshot会从磁盘中反序列化
async fn install_full_snapshot(
    app: Arc<CacheCatApp>,
    req: InstallFullSnapshotReq,
) -> Result<SnapshotResponse<TypeConfig>, String> {
    let snapshot = Snapshot {
        meta: req.snapshot_meta,
        snapshot: req.snapshot,
    };
    app.raft
        .install_full_snapshot(req.vote, snapshot)
        .await
        .map_err(|e| e.to_string())
}

async fn add_node(app: Arc<CacheCatApp>, req: JoinRequest) -> Result<(), String> {
    let node = Node {
        node_id: req.node_id,
        endpoint: req.endpoint.clone(),
    };
    // 已经存在就不继续加入
    let existed = app.raft.voter_ids().any(|id| id == node.node_id);
    if existed {
        info!("node {} already exists", node.node_id);
        return Ok(());
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
    Ok(())
}
