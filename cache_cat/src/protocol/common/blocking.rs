use crate::protocol::command::Client;
use tokio::io::AsyncReadExt;

/// Preserve pipelined input for the connection loop while detecting EOF.
/// Dropping the competing read future also drops its blocking registration.
pub(crate) async fn buffer_until_disconnect(client: &mut Client) -> std::io::Result<()> {
    let mut buffer = [0; 4096];
    loop {
        let len = client.framed.get_mut().read(&mut buffer).await?;
        if len == 0 {
            return Ok(());
        }
        client
            .framed
            .read_buffer_mut()
            .extend_from_slice(&buffer[..len]);
    }
}
