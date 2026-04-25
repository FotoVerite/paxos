pub const RUST_EMOJI_POOL: &[&str] = &["🦀", "📦", "🔒", "🧵", "⚙️", "🧪"];

pub fn is_valid_rust_emoji(emoji: &str) -> bool {
    RUST_EMOJI_POOL.contains(&emoji)
}
