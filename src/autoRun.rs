use sqlx::SqliteConnection;
use sqlx::Connection;
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
}

async fn get_connection() -> anyhow::Result<SqliteConnection> {
    Ok(SqliteConnection::connect("sqlite:database.db?mode=rwc").await?)
}

pub async fn insert_connection(connection: connection) -> anyhow::Result<()> {
    let mut conn = get_connection().await?;
    sqlx::query("INSERT INTO connection VALUES (?, ?, ?, ?, ?)")
        .bind(&connection.connection_id)
        .bind(connection.connection_type.to_i32())
        .bind(&connection.connection_point)
        .bind(&connection.username)
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
) -> anyhow::Result<()> {
    let mut conn = get_connection().await?;
    sqlx::query(
        "INSERT INTO mounts (host_drive, local_drive, connection_id)
         VALUES (?, ?, (
             SELECT id FROM connection
             WHERE connection_type = ? AND connection_point = ? AND username = ?
         ))",
    )
        .bind(host_drive)
        .bind(mount_point)
        .bind(connection_type.to_i32())
        .bind(connection_point)
        .bind(username)
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

pub async fn get_connection_from_db(
    connection_type: ConnectionType,
    connection_point: String,
    username: String,
    scope: String,
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
        connection_point,
        username,
        scope,
        key: row.get("key"),
        mounts: None,
    })
}

pub async fn remove_connection(
    connection_type: ConnectionType,
    connection_point: String,
    username: String,
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

pub async fn update_scope(
    connection_type: ConnectionType,
    connection_point: String,
    username: String,
    drive: String,
    hostdrive: String,
    scope: String,
) -> anyhow::Result<()> {
    let mut conn = get_connection().await?;

    sqlx::query(
        "UPDATE mounts
         SET scope = ?1
         WHERE local_drive = ?2
           AND host_drive = ?3
           AND connection_id = (
               SELECT id FROM connection
               WHERE connection_type = ?4
                 AND connection_point = ?5
                 AND username = ?6
           )",
    )
        .bind(&scope)
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

pub async fn get_scope(
    connection_type: ConnectionType,
    connection_point: String,
    username: String,
    host_drive: String,
) -> anyhow::Result<String> {
    let mut conn = get_connection().await?;

    use sqlx::Row;
    let row = sqlx::query(
        "SELECT mounts.scope FROM connection
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

    Ok(row.get("scope"))
}

pub async fn init_db() -> anyhow::Result<()> {
    let mut conn = get_connection().await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS connection (
            id TEXT NOT NULL PRIMARY KEY,
            connection_type INTEGER NOT NULL CHECK(connection_type IN (0, 1)),
            connection_point TEXT NOT NULL,
            username TEXT NOT NULL,
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
            scope TEXT,
            UNIQUE(host_drive, connection_id),
            FOREIGN KEY (connection_id) REFERENCES connection(id)
        )",
    )
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
