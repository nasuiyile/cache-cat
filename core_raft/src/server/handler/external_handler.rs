use crate::error::{CoreRaftError, PinBoxFutureStatic};
use crate::network::model::{WriteReq, WriteRes};
use crate::network::raft_rocksdb::{CacheCatApp, TypeConfig};
use crate::server::handler::model::{
    AppendEntriesReq, AppendEntriesRes, DelReq, DelRes, ExistsReq, ExistsRes, GetReq, GetRes,
    InstallFullSnapshotReq, InstallFullSnapshotRes, PrintTestReq, PrintTestRes, ReadReq, ReadRes,
    SetReq, SetRes, VoteReq, VoteRes,
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
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::LazyLock;
use std::time::Instant;

macro_rules! static_handler_table {
	( $(#[$attr:meta])* $vis:vis $enum_name:ident -> <$value_type:ty, $error_type:ty> { $( $variant:ident => $variant_value:expr ),* $(,)? }, $try_into_err:path ) => {
		macro_rules! __impl {
			($variant2:ident, $variant_value2:expr, EnumVariantValueCheck) => {
				paste::paste! {
					struct [<$variant2 ValueCheck>];

					impl [<$variant2 ValueCheck>] {
						const TYPE_CHECKER_VALUE: $value_type = [<$variant2 ValueCheck>]::type_checker($variant_value2);

						const fn type_checker<T: num_traits::Unsigned>(v: T) -> T {
							v
						}
					}
				}
		    };
			($variant2:ident, $variant_value2:expr, EnumVariant) => {
		        $(#[$attr])*
		        $vis struct $variant2;

		        impl $variant2 {
					$vis const VALUE: $value_type = $variant_value2;
		        }

		        paste::paste! {
		            impl [<$enum_name Typed>] for $variant2 {
						type Enum = $enum_name;
		                type Request = [<$variant2 Req>];
		                type Response = [<$variant2 Res>];
						type Fut = PinBoxFutureStatic<Self::Response>;

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
            	$variant/* = $variant_value*/,
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
						_ => Err($try_into_err(self)),
					}
				}
			}

			$vis trait [<$enum_name Typed>]: Send + Sync {
				type Enum;
				type Request: serde::Serialize;
				type Response: serde::de::DeserializeOwned;
				type Fut: Future<Output = Self::Response> + Send + 'static;

				fn value() -> $value_type;
			}

			$vis mod [<$enum_name:snake _typed>] {
				use super::*;

				$(
			    	__impl!($variant, $variant_value, EnumVariantValueCheck);
				)?

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
			let handlers = std::array::from_fn(|i| {
				match i {
					$(
						$variant_value => {
							paste::paste! {
								Some(Box::new(RpcMethod {
									func: [<$variant:snake>],
									_phantom: PhantomData::<[<$enum_name:snake _typed>]::$variant>,
								}) as BoxRpcHandler)
							}
						},
					)+
					_ => None,
				}
			});
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
    pub FuncId -> <u32, CoreRaftError> {
        PrintTest => 1,
        Write => 2,
        Read => 3,
        Vote => 6,
        AppendEntries => 7,
        InstallFullSnapshot => 8,
    }, CoreRaftError::RpcInvalidFuncId
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
pub struct RpcMethod<T, Fut>
where
    T: FuncIdTyped,
    Fut: Future<Output = T::Response>,
{
    // 注意：Rust 的纯函数指针 fn 不能直接是 async 的
    // 我们这里让 func 返回一个 Future
    func: fn(Arc<CacheCatApp>, T::Request) -> Fut,
    _phantom: PhantomData<T>,
}

#[async_trait]
impl<T, Fut> RpcHandler for RpcMethod<T, Fut>
where
    T: FuncIdTyped,
    Fut: Future<Output = T::Response> + Send,
    T::Request: DeserializeOwned,
    T::Response: Serialize,
{
    async fn call(&self, app: Arc<CacheCatApp>, data: Bytes) -> Bytes {
        // 反序列化
        let req: T::Request = bincode2::deserialize(data.as_ref()).expect("Failed to deserialize");
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

async fn write(app: Arc<CacheCatApp>, req: WriteReq) -> WriteRes {
    let res = app.raft.client_write(req).await.expect("Raft write failed");
    res
}
async fn read(app: Arc<CacheCatApp>, req: ReadReq) -> ReadRes {
    let kvs = app.key_values.lock().await;
    let value = kvs.get(&req.data);
    ReadRes {
        value: value.map(|v| v.to_string()),
    }
}

async fn vote(app: Arc<CacheCatApp>, req: VoteReq) -> VoteRes {
    // openraft 的 vote 是异步的
    VoteRes {
        value: app.raft.vote(req.data).await.expect("Raft vote failed"),
    }
}

//理论上只有从节点会被调用这个方法
async fn append_entries(app: Arc<CacheCatApp>, req: AppendEntriesReq) -> AppendEntriesRes {
    let start = Instant::now();
    let e = req.data.entries.is_empty();
    let res = app
        .raft
        .append_entries(req.data)
        .await
        .expect("Raft append_entries failed");
    let elapsed = start.elapsed();
    if !e {
        tracing::info!("append 从节点内部处理: {:?} 节点：{:?}", elapsed, app.id);
    }
    AppendEntriesRes { value: res }
}

//InstallFullSnapshotReq 把openraft自带的俩个参数包裹在一起了
async fn install_full_snapshot(
    app: Arc<CacheCatApp>,
    req: InstallFullSnapshotReq,
) -> InstallFullSnapshotRes {
    let snapshot = Snapshot {
        meta: req.snapshot_meta,
        snapshot: req.snapshot.clone(),
    };
    InstallFullSnapshotRes {
        value: app
            .raft
            .install_full_snapshot(req.vote, snapshot)
            .await
            .expect("Raft install_snapshot failed"),
    }
}
