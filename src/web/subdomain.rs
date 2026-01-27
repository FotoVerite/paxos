use axum::{
    extract::FromRequestParts,
    http::request::Parts,
};

#[derive(Clone, Debug)]
pub struct Subdomain(pub String);

#[async_trait::async_trait]
impl<S> FromRequestParts<S> for Subdomain
where
    S: Send + Sync,
{
    type Rejection = String;

    async fn from_request_parts(parts: &mut Parts, _: &S) -> Result<Self, Self::Rejection> {
        let host = parts
            .headers
            .get("host")
            .and_then(|h| h.to_str().ok())
            .unwrap_or("localhost");

        let subdomain = host
            .split(':')
            .next()
            .and_then(|h| h.split('.').next())
            .unwrap_or("localhost")
            .to_string();

        Ok(Subdomain(subdomain))
    }
}
