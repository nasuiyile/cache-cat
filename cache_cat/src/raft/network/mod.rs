pub mod client;
pub mod connection;
pub mod external_handler;
pub mod model;
pub mod pipeline_client;
pub mod raft_network;
pub mod redis_server;
pub mod rpc;
pub mod tls;

use tokio_util::codec::LengthDelimitedCodec;

/// 单个 RPC 帧的最大长度。
/// LengthDelimitedCodec 默认上限是 8MB（编码端和解码端都会强制执行）。
/// 主节点给落后太多的从节点批量同步日志时，单帧很容易超过 8MB，
/// 注意：该值需要与 openraft 的 max_payload_entries 配置相匹配，
/// 保证一批日志序列化后不会超过此上限。
pub const MAX_FRAME_LENGTH: usize = 256 * 1024 * 1024 * 1024; //256GB

/// 全项目统一的帧编解码器构造函数，收发两端必须使用相同的上限。
pub fn new_length_codec() -> LengthDelimitedCodec {
    LengthDelimitedCodec::builder()
        .max_frame_length(MAX_FRAME_LENGTH)
        .new_codec()
}
