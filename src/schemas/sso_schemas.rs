use serde::{Deserialize, Serialize};
use sqlx::{FromRow, PgPool, postgres::PgQueryResult, query, query_as};

/// How long a partner app has to redeem a code after InsIIT issues it.
pub const CODE_LIFETIME_SECONDS: i32 = 30;

#[derive(Deserialize)]
pub struct CodeRequest {
    pub client_id: String,
    pub redirect_uri: String,
}

#[derive(Serialize)]
pub struct CodeResponse {
    pub redirect_url: String,
}

#[derive(Deserialize)]
pub struct ExchangeRequest {
    pub client_id: String,
    pub code: String,
}

#[derive(FromRow)]
pub struct RedeemedCode {
    pub uid: String,
    pub email: String,
}

#[derive(Serialize)]
pub struct ExchangeResponse {
    pub uid: String,
    pub email: String,
    /// For apps on our Firebase project: pass to signInWithCustomToken.
    pub firebase_custom_token: String,
}

pub async fn initialize_table(pool: &PgPool) -> Result<PgQueryResult, sqlx::Error> {
    query(
        "
        CREATE TABLE IF NOT EXISTS sso_clients (
            client_id VARCHAR(64) PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            redirect_uris TEXT[] NOT NULL DEFAULT '{}'
        );
    ",
    )
    .execute(pool)
    .await?;
    query(
        "
        CREATE TABLE IF NOT EXISTS sso_codes (
            code VARCHAR(64) PRIMARY KEY,
            client_id VARCHAR(64) NOT NULL REFERENCES sso_clients(client_id) ON DELETE CASCADE,
            uid VARCHAR(128) NOT NULL,
            email VARCHAR(255) NOT NULL,
            expires_at TIMESTAMPTZ NOT NULL
        );
    ",
    )
    .execute(pool)
    .await
}

/// Redirects only go to URIs registered for the app, matched exactly.
pub async fn redirect_uri_registered(
    pool: &PgPool,
    client_id: &str,
    redirect_uri: &str,
) -> Result<bool, sqlx::Error> {
    query("SELECT 1 FROM sso_clients WHERE client_id = $1 AND $2 = ANY(redirect_uris);")
        .bind(client_id)
        .bind(redirect_uri)
        .fetch_optional(pool)
        .await
        .map(|row| row.is_some())
}

pub async fn create_code(
    pool: &PgPool,
    code: &str,
    client_id: &str,
    uid: &str,
    email: &str,
) -> Result<(), sqlx::Error> {
    // Codes live for seconds, so clearing out expired ones here keeps the table small.
    query("DELETE FROM sso_codes WHERE expires_at < NOW();")
        .execute(pool)
        .await?;
    query(
        "INSERT INTO sso_codes (code, client_id, uid, email, expires_at)
        VALUES ($1, $2, $3, $4, NOW() + make_interval(secs => $5));",
    )
    .bind(code)
    .bind(client_id)
    .bind(uid)
    .bind(email)
    .bind(CODE_LIFETIME_SECONDS)
    .execute(pool)
    .await?;
    Ok(())
}

/// Deleting the row as it is read makes each code single-use.
pub async fn redeem_code(
    pool: &PgPool,
    client_id: &str,
    code: &str,
) -> Result<Option<RedeemedCode>, sqlx::Error> {
    query_as::<_, RedeemedCode>(
        "DELETE FROM sso_codes WHERE code = $1 AND client_id = $2 AND expires_at > NOW()
        RETURNING uid, email;",
    )
    .bind(code)
    .bind(client_id)
    .fetch_optional(pool)
    .await
}
