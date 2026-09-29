use super::ContextualUserFragment;
use codex_protocol::models::ContentItemKind;

/// Bounded model guidance for threads whose tool catalog is owned by DSH.
pub(crate) struct DelegatedToolsInstructions;

impl ContextualUserFragment for DelegatedToolsInstructions {
    fn content_kind(&self) -> ContentItemKind {
        ContentItemKind("model.delegated_tools_instructions".to_string())
    }

    fn role(&self) -> &'static str {
        "developer"
    }

    fn markers(&self) -> (&'static str, &'static str) {
        Self::type_markers()
    }

    fn type_markers() -> (&'static str, &'static str) {
        ("", "")
    }

    fn body(&self) -> String {
        "Tool execution for this session is delegated to DeepSeek Harness (DSH). \
         Use only function tools actually supplied in the current model request. \
         DSH owns their implementation, policy, approval, execution, and results. \
         Do not assume native Codex shell, patch, MCP, web, or delegation tools are available. \
         When such work is needed, select a corresponding supplied DSH function. \
         If no suitable function is supplied, explain the limitation; never invent a tool name or claim execution. \
         The supplied tool catalog may change between turns."
            .to_string()
    }
}
