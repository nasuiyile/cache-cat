mod external_handler;
pub mod model;
pub mod rpc;

pub mod prelude {
    pub use super::external_handler::{BoxStdError, FuncId, FuncIdTyped, get_handler, func_id_typed};
}