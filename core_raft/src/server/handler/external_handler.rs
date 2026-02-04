use crate::network::model::{WriteReq, WriteRes};
use crate::network::raft_rocksdb::{CacheCatApp, TypeConfig};
use crate::server::handler::model::{
    DelReq, DelRes, ExistsReq, ExistsRes, GetReq, GetRes, InstallFullSnapshotReq, PrintTestReq,
    PrintTestRes, SetReq, SetRes,
    InstallFullSnapshotRes, AppendEntriesReq, AppendEntriesRes, VoteReq, VoteRes, ReadReq, ReadRes,
};
use async_trait::async_trait;
use bytes::Bytes;
use openraft::Snapshot;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, ClientWriteResponse, InstallSnapshotRequest,
    InstallSnapshotResponse, SnapshotResponse, VoteRequest, VoteResponse,
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::io::Cursor;
use std::sync::Arc;
use std::time::Instant;
use std::sync::LazyLock;

pub type BoxStdError = Box<dyn std::error::Error>;

macro_rules! static_handler_table {
	( $(#[$attr:meta])* $vis:vis $enum_name:ident -> <$value_type:ty, $error_type:ty> { $( $variant:ident => $variant_value:expr ),* $(,)? } ) => {
		macro_rules! __impl {
			($variant2:ident, $variant_value2:expr, EnumVariant) => {
		        $(#[$attr])*
		        $vis struct $variant2;

		        impl $variant2 {
		            $vis const VALUE: $value_type = $variant_value2;
		        }

		        paste::paste! {
		            impl [<$enum_name Typed>] for $variant2 {
		                type Request = [<$variant2 Req>];
		                type Response = [<$variant2 Res>];

		                fn value() -> $value_type {
		                    Self::VALUE
		                }
		            }
		        }
		    };
		}
		
		#[allow(non_camel_case_types)]
        $(#[$attr])*
        $vis enum $enum_name {
            $(
            	$variant = $variant_value,
	    	)+
        }
		
		impl $enum_name {
	        fn value(&self) -> $value_type {
	            match self {
	            	$(
	                	$enum_name::$variant => $variant_value as $value_type,
					)+
	            }
	        }
	    }
		
		paste::paste! {
			impl TryInto<$enum_name> for $value_type {
				type Error = $error_type;

				fn try_into(self) -> Result<$enum_name, Self::Error> {
					match self {
						$(
							$variant_value => Ok($enum_name::$variant),
						)*
						_ => Err(format!("Invalid function ID: {} <{}>", self, stringify!($value_type)).into()),
					}
				}
			}
		
			$vis trait [<$enum_name Typed>] {
				type Request: serde::Serialize;
				type Response: serde::de::DeserializeOwned;
				
				fn value() -> $value_type;
			}
		
			$vis mod [<$enum_name:snake _typed>] {
				use super::*;
				
				$(
			    	__impl!($variant, $variant_value, EnumVariant);
				)+
			}
		}

		const HANDLER_TABLE_LEN: usize = {
			let mut max = 0;
			$(
				if $variant_value > max { max = $variant_value; }
			)+
			max + 1
		};

        type BoxRpcHandler = Box<dyn RpcHandler>;
		type OptionBoxRpcHandler = Option<BoxRpcHandler>;
		static HANDLER_TABLE: LazyLock<[OptionBoxRpcHandler; HANDLER_TABLE_LEN]> = LazyLock::new(|| {
			let mut handlers: [OptionBoxRpcHandler; HANDLER_TABLE_LEN] = std::array::from_fn(|_| None);
			$(
				paste::paste! {
					handlers[$variant_value] = Some(Box::new(RpcMethod { func: [<$variant:snake>] }));
				}
			)+
			handlers
		});
		
		#[inline]
		pub fn get_handler(variant: $enum_name) -> Option<&'static dyn RpcHandler> {
			HANDLER_TABLE.get(variant.value() as usize)?.as_ref().map(|boxed| boxed.as_ref())
		}
	}
}

static_handler_table! {
	#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
	pub FuncId -> <u32, BoxStdError> {
		PrintTest => 1,
		Write => 2,
		Read => 3,
		Vote => 6,
		AppendEntries => 7,
		InstallFullSnapshot => 8,
	}
}

/*
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
*/

#[async_trait]
pub trait RpcHandler: Send + Sync {
    // 将 app 改为 Arc 传递，更符合异步环境下的生命周期要求
    async fn call(&self, app: Arc<CacheCatApp>, data: Bytes) -> Bytes;
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
    async fn call(&self, app: Arc<CacheCatApp>, data: Bytes) -> Bytes {
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

async fn print_test(_app: Arc<CacheCatApp>, d: PrintTestReq) -> PrintTestRes {
    PrintTestRes { message: d.message }
}

async fn write(app: Arc<CacheCatApp>, req: WriteReq) -> ClientWriteResponse<TypeConfig> {
    let res: ClientWriteResponse<TypeConfig> =
        app.raft.client_write(req).await.expect("Raft write failed");
    return res;
}
async fn read(app: Arc<CacheCatApp>, req: String) -> Option<String> {
    let kvs = app.key_values.lock().await;
    let value = kvs.get(&req);
    value.map(|v| v.to_string())
}

async fn vote(app: Arc<CacheCatApp>, req: VoteRequest<TypeConfig>) -> VoteResponse<TypeConfig> {
    // openraft 的 vote 是异步的
    app.raft.vote(req).await.expect("Raft vote failed")
}

//理论上只有从节点会被调用这个方法
async fn append_entries(
    app: Arc<CacheCatApp>,
    req: AppendEntriesRequest<TypeConfig>,
) -> AppendEntriesResponse<TypeConfig> {
    let start = Instant::now();
    let e = req.entries.is_empty();
    let res = app
        .raft
        .append_entries(req)
        .await
        .expect("Raft append_entries failed");
    let elapsed = start.elapsed();
    if !e {
        tracing::info!("append 从节点内部处理: {:?} 节点：{:?}", elapsed, app.id);
    }
    res
}

//InstallFullSnapshotReq 把openraft自带的俩个参数包裹在一起了
async fn install_full_snapshot(
    app: Arc<CacheCatApp>,
    req: InstallFullSnapshotReq,
) -> SnapshotResponse<TypeConfig> {
    let snapshot = Snapshot {
        meta: req.snapshot_meta,
        snapshot: Cursor::new(req.snapshot),
    };
    app.raft
        .install_full_snapshot(req.vote, snapshot)
        .await
        .expect("Raft install_snapshot failed")
}
