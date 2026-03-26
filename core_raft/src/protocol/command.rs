use std::collections::HashMap;
use async_trait::async_trait;
use crate::network::model::Response;

#[async_trait]
pub trait Command: Send + Sync {
    /// Execute the command with given RESP items and server context
    async fn execute(&self, items: &[Response], server: &Response) -> Response;
}

/// Command factory for creating and executing commands
pub struct CommandFactory {
    commands: HashMap<String, Box<dyn Command>>,
}

impl CommandFactory {
    /// Create a new empty command factory
    fn new() -> Self {
        Self {
            commands: HashMap::new(),
        }
    }

    /// Register a command with given name
    fn register<C: Command + 'static>(&mut self, name: impl Into<String>, cmd: C) {
        self.commands.insert(name.into(), Box::new(cmd));
    }

    /// Initialize the command factory with all supported commands
    pub fn init() -> Self {
        let mut factory = Self::new();

        // Register connection commands


        factory
    }

}
