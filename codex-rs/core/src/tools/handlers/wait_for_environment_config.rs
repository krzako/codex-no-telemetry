/// Model-visible descriptions supplied by a host that supports deferred environments.
///
/// Retained for configuration compatibility while the native `wait_for_environment`
/// tool implementation is removed from the Codex fork.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WaitForEnvironmentToolConfig {
    /// Explains when and why the model should call `wait_for_environment`.
    pub tool_description: String,
    /// Explains how the model should select the `environment_id` argument.
    pub environment_id_description: String,
}
