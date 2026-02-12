use anyhow::{Context, Result};

#[derive(Clone, Debug)]
pub struct ServiceConfig {
    pub database_url: String,
    pub redis_url: String,
    pub http_addr: String,
}

impl ServiceConfig {
    pub fn from_env(default_http_addr: &str) -> Result<Self> {
        let database_url = std::env::var("DATABASE_URL").context("DATABASE_URL is required")?;
        let redis_url = std::env::var("REDIS_URL").context("REDIS_URL is required")?;
        let http_addr =
            std::env::var("HTTP_ADDR").unwrap_or_else(|_| default_http_addr.to_string());

        Ok(Self {
            database_url,
            redis_url,
            http_addr,
        })
    }

    pub fn worker_from_env() -> Result<Self> {
        let database_url = std::env::var("DATABASE_URL").context("DATABASE_URL is required")?;
        let redis_url = std::env::var("REDIS_URL").context("REDIS_URL is required")?;

        Ok(Self {
            database_url,
            redis_url,
            http_addr: String::new(),
        })
    }
}
