use sqlx::{Row, SqliteConnection};
use sqlx::Connection;
use rusqlite;
use x509_parser::asn1_rs::Boolean;
use x509_parser::nom::complete::bool;
use crate::driveManagerUI::ConnectionType;

pub struct connection {
    pub connection_id: String,
    pub connection_type: ConnectionType,
    pub connection_point: String,
    pub username: String,
    pub key: Vec<u8>,
    pub scope: String,
    pub mounts: Option<Vec<mounts>>,
}

pub struct mounts {
    pub host_drive: String,
    pub mount_point: String,
    pub scope: String,
    pub connection: Option<connection>,
    pub compress: bool
}

async fn get_connection() -> anyhow::Result<SqliteConnection> {
    Ok(SqliteConnection::connect("sqlite:vaultDrive_client.db?mode=rwc").await?)
}
// Sync rusqlite - uses file path (not connection string)
fn get_connection_sync() -> anyhow::Result<rusqlite::Connection> {
    Ok(rusqlite::Connection::open("vaultDrive_client.db")?)
}

pub async fn insert_connection(connection: connection) -> anyhow::Result<()> {
    let mut conn = get_connection().await?;
    sqlx::query("INSERT INTO connection (id, connection_type, connection_point, username, scope, key) VALUES (?, ?, ?, ?, ?, ?)")
        .bind(&connection.connection_id)
        .bind(connection.connection_type.to_i32())
        .bind(&connection.connection_point)
        .bind(&connection.username)
        .bind(&connection.scope)
        .bind(&connection.key)
        .execute(&mut conn)
        .await?;
    Ok(())
}

pub async fn insert_mount(
    connection_type: ConnectionType,
    connection_point: &str,
    username: &str,
    mount_point: &str,
    host_drive: &str,
    compress: &bool,
    scope: &str,
) -> anyhow::Result<()> {
    let mut conn = get_connection().await?;
    sqlx::query(
        "INSERT INTO mounts (host_drive, local_drive, connection_id, compress, scope)
         VALUES (?, ?, (
             SELECT id FROM connection
             WHERE connection_type = ? AND connection_point = ? AND username = ?
         ), ?, ?)",
    )
        .bind(host_drive)
        .bind(mount_point)
        .bind(connection_type.to_i32())
        .bind(connection_point)
        .bind(username)
        .bind(compress)
        .bind(scope)
        .execute(&mut conn)
        .await?;
    Ok(())
}

pub async fn remove_mount(
    connection_type: ConnectionType,
    connection_point: &str,
    username: &str,
    mount_point: &str,
) -> anyhow::Result<()> {
    let mut conn = get_connection().await?;
    sqlx::query(
        "DELETE FROM mounts
         WHERE local_drive = ?
         AND connection_id = (
             SELECT id FROM connection
             WHERE connection_type = ? AND connection_point = ? AND username = ?
         )",
    )
        .bind(mount_point)
        .bind(connection_type.to_i32())
        .bind(connection_point)
        .bind(username)
        .execute(&mut conn)
        .await?;
    Ok(())
}
pub async fn update_connect_key(id: &str, key: &[u8]) -> anyhow::Result<()> {
    let mut conn = get_connection().await?;
    sqlx::query("UPDATE connection SET key = ? WHERE id = ?")
        .bind(key)
        .bind(id)
        .execute(&mut conn)
        .await?;
    Ok(())
}

pub async fn get_connection_from_db(
    connection_type: ConnectionType,
    connection_point: &str,
    username: &str,
    scope: &str,
) -> anyhow::Result<connection> {
    let mut conn = get_connection().await?;

    use sqlx::Row;
    let row = sqlx::query(
        "SELECT id, key FROM connection
         WHERE connection_type = ? AND connection_point = ? AND username = ? AND scope = ?",
    )
        .bind(connection_type.to_i32())
        .bind(&connection_point)
        .bind(&username)
        .bind(&scope)
        .fetch_one(&mut conn)
        .await?;

    Ok(connection {
        connection_id: row.get("id"),
        connection_type,
        connection_point: connection_point.to_string(),
        username: username.to_string(),
        scope: scope.to_string(),
        key: row.get("key"),
        mounts: None,
    })
}

pub async fn remove_connection(
    connection_type: ConnectionType,
    connection_point: String,
    username: &str,
) -> anyhow::Result<Option<String>> {
    let mut conn = get_connection().await?;
    let mut tx = conn.begin().await?;

    let connection_id: Option<String> = sqlx::query_scalar(
        "SELECT id FROM connection
         WHERE connection_type = ? AND connection_point = ? AND username = ?",
    )
        .bind(connection_type.to_i32())
        .bind(&connection_point)
        .bind(&username)
        .fetch_optional(&mut *tx)
        .await?;

    let Some(connection_id) = connection_id else {
        return Ok(None);
    };

    sqlx::query("DELETE FROM mounts WHERE connection_id = ?")
        .bind(&connection_id)
        .execute(&mut *tx)
        .await?;

    sqlx::query("DELETE FROM connection WHERE id = ?")
        .bind(&connection_id)
        .execute(&mut *tx)
        .await?;

    tx.commit().await?;
    Ok(Some(connection_id))
}

pub async fn update_scope_and_compress(
    connection_type: ConnectionType,
    connection_point: &str,
    username: &str,
    drive: &str,
    hostdrive: &str,
    scope: &str,
    compress: bool,
) -> anyhow::Result<()> {
    let mut conn = get_connection().await?;

    sqlx::query(
        "UPDATE mounts
         SET scope = ?1
         SET compress = ?2
         WHERE local_drive = ?3
           AND host_drive = ?4
           AND connection_id = (
               SELECT id FROM connection
               WHERE connection_type = ?5
                 AND connection_point = ?6
                 AND username = ?7
           )",
    )
        .bind(&scope)
        .bind(compress)
        .bind(&drive)
        .bind(&hostdrive)
        .bind(connection_type as i32)
        .bind(&connection_point)
        .bind(&username)
        .execute(&mut conn)
        .await?;

    Ok(())
}

pub async fn get_connections_with_mounts() -> anyhow::Result<Vec<connection>> {
    let mut conn = get_connection().await?;

    use sqlx::Row;
    let rows = sqlx::query(
        "SELECT
            c.id,
            c.connection_type,
            c.connection_point,
            c.username,
            c.key,
            c.scope AS conn_scope,
            m.host_drive,
            m.local_drive,
            m.scope AS mount_scope
         FROM connection c
         JOIN mounts m ON m.connection_id = c.id
         ORDER BY c.connection_type, c.connection_point, c.username",
    )
        .fetch_all(&mut conn)
        .await?;

    let mut connections: Vec<connection> = Vec::new();

    for row in rows {
        let id: String = row.get("id");
        let mount = mounts {
            host_drive: row.get("host_drive"),
            mount_point: row.get("local_drive"),
            scope: row.get("mount_scope"),
            compress: row.get("compress"),
            connection: None,
        };

        match connections.iter_mut().find(|c| c.connection_id == id) {
            Some(c) => {
                c.mounts.get_or_insert_with(Vec::new).push(mount);
            }
            None => {
                connections.push(connection {
                    connection_id: id,
                    connection_type: ConnectionType::from_i32(row.get("connection_type"))?,
                    connection_point: row.get("connection_point"),
                    username: row.get("username"),
                    key: row.get("key"),
                    scope: row.get("conn_scope"),
                    mounts: Some(vec![mount]),
                });
            }
        }
    }

    Ok(connections)
}

pub async fn get_scope_and_compress(
    connection_type: ConnectionType,
    connection_point: String,
    username: String,
    host_drive: String,
) -> anyhow::Result<(String, bool)> {
    let mut conn = get_connection().await?;

    use sqlx::Row;
    let row = sqlx::query(
        "SELECT mounts.scope AND mounts.compress FROM connection
         JOIN mounts ON mounts.connection_id = connection.id
         WHERE connection.connection_type = ?
           AND connection.connection_point = ?
           AND connection.username = ?
           AND mounts.host_drive = ?",
    )
        .bind(connection_type.to_i32())
        .bind(&connection_point)
        .bind(&username)
        .bind(&host_drive)
        .fetch_one(&mut conn)
        .await?;

    Ok((row.get("scope"), row.get("compress")))
}


pub fn contain_cert(cert_bytes: &[u8]) -> anyhow::Result<bool> {
    let conn = get_connection_sync()?;
    let rows_updated = conn.execute(
        "UPDATE key_store SET last_seen = CURRENT_TIMESTAMP WHERE server_cert = ?",
        [cert_bytes],
    )?;
    Ok(rows_updated >= 1)
}

pub fn insert_cert(cert_bytes: &[u8]) -> anyhow::Result<()> {
    let conn = get_connection_sync()?;
    conn.execute(
        "INSERT INTO key_store (server_cert) VALUES (?)
         ON CONFLICT(server_cert) DO UPDATE SET last_seen = CURRENT_TIMESTAMP",
        [cert_bytes],
    )?;
    Ok(())
}


pub async fn init_db() -> anyhow::Result<()> {
    let mut conn = get_connection().await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS connection (
            id TEXT NOT NULL PRIMARY KEY,
            connection_type INTEGER NOT NULL CHECK(connection_type IN (0, 1)),
            connection_point TEXT NOT NULL,
            username TEXT NOT NULL,
            scope text,
            key BLOB NOT NULL,
            UNIQUE(connection_type, connection_point, username)
        )",
    )
        .execute(&mut conn)
        .await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS mounts (
            host_drive TEXT NOT NULL,
            local_drive TEXT NOT NULL UNIQUE,
            connection_id TEXT NOT NULL,
            scope TEXT ,
            compress BOOLEAN NOT NULL,
            UNIQUE(host_drive, connection_id),
            FOREIGN KEY (connection_id) REFERENCES connection(id)
        )",
    )
        .execute(&mut conn)
        .await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS key_store (
    server_cert BLOB NOT NULL UNIQUE,
    first_seen TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_seen TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
)",)
        .execute(&mut conn)
        .await?;

    sqlx::query(
        "DELETE FROM connection
         WHERE NOT EXISTS (
             SELECT 1 FROM mounts WHERE mounts.connection_id = connection.id
         )",
    )
        .execute(&mut conn)
        .await?;

    Ok(())
}
