use auto_launch::*;
use rusqlite::{Connection};
use crate::driveManagerUI::{ConnectionType};

#[derive(Debug, PartialEq, )]
pub struct auto_run {
    pub connection_type: ConnectionType,
    pub connection_point: String,
    pub username: String,
    pub host_drive: String,
    pub mount_point: String,
}
impl auto_run {
    pub fn new(
        connection_type: ConnectionType,
        connection_point: String,
        username: String,
        host_drive: String,
        mount_point: String
    ) -> Self {
        Self {
            connection_type,
            connection_point,
            username,
            host_drive,
            mount_point,
        }
    }
}



fn get_connection() -> rusqlite::Result<Connection> {
    Connection::open("database.db")
}

fn count() -> anyhow::Result<i64> {
    let connection = get_connection()?;
    let query = "SELECT COUNT(*) FROM auto_run";

    let count = connection.query_row(query, [], |row| row.get(0))?;

    Ok(count)
}

pub fn get_auto_run() -> anyhow::Result<Vec<auto_run>> {
    let connection = get_connection()?;
    let query = "SELECT connection_type, connection_point, username, host_drive, local_drive FROM auto_run";

    let mut stmt = connection.prepare(query)?;

    stmt.query_map([], |row| {
        Ok((
            row.get::<_, i32>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
           row.get::<_, String>(3)?,
            row.get::<_, String>(4)?,

        ))
    })?
        .map(|result| {
            let (connection_type_int, connection_point, username, host_drive, local_drive) = result?;
            let connection_type = ConnectionType::from_i32(connection_type_int)?;
            Ok(auto_run {
                connection_type,
                connection_point,
                username,
                host_drive,
                mount_point: local_drive,
            })
        })
        .collect()
}
pub fn insert(auto_run: auto_run) -> anyhow::Result<()> {
    let connection = get_connection()?;
    let count_before = count()?;

    connection.execute(
        "INSERT INTO auto_run (connection_type, connection_point, username, host_drive, local_drive) VALUES (?, ?, ?, ?, ?)",
        rusqlite::params![
            auto_run.connection_type.to_i32(),
            auto_run.connection_point,
            auto_run.username,
            auto_run.host_drive,
            auto_run.mount_point,
        ],
    )?;

    if count_before == 0 {
        enable_auto_run()?;
    }

    Ok(())
}

pub fn remove(auto_run: auto_run) -> anyhow::Result<()> {
    let connection = get_connection()?;

    let query = "DELETE FROM auto_run WHERE connection_type = ? AND connection_point = ? AND username = ?";

    connection.execute(
        query,
        rusqlite::params![
            auto_run.connection_type.to_string(),
            auto_run.connection_point,
            auto_run.username
        ],
    )?;
    let count = count()?;

    if count == 0 {
        disable_auto_run()?;
    }


    Ok(())
}
pub fn exists(auto_run: auto_run) -> anyhow::Result<bool> {
    let connection = get_connection()?;
    let query = "SELECT 1 FROM auto_run WHERE connection_type = ? AND connection_point = ? AND username = ? AND host_drive = ?";

    let result = connection.query_row(
        query,
        rusqlite::params![
            auto_run.connection_type.to_i32(),
            auto_run.connection_point,
            auto_run.username,
            auto_run.host_drive,
        ],
        |_row| Ok(true),
    );

    match result {
        Ok(exists) => Ok(exists),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(false),
        Err(e) => Err(e.into()),
    }
}

pub fn create_table() -> anyhow::Result<()> {
    let connection = get_connection()?;

    let query = "
        CREATE TABLE IF NOT EXISTS auto_run (
            connection_type INTEGER NOT NULL CHECK(connection_type IN (0, 1)),
            connection_point TEXT NOT NULL,
            username TEXT NOT NULL,
            drive TEXT NOT NULL
            UNIQUE(connection_type, connection_point, username, host_drive, local_drive)
        );
    ";
    connection.execute(query, [])?;

    Ok(())
}

fn enable_auto_run() -> anyhow::Result<()>{
    let auto = get_auto_launch()?;
    auto.enable()?;
    Ok(())

}
fn disable_auto_run() -> anyhow::Result<()>{
    let auto = get_auto_launch()?;
    auto.disable()?;
    Ok(())


}
fn get_auto_launch() -> anyhow::Result<AutoLaunch> {
    let app_name = "vaultDrive";
    let app_path = std::env::current_exe()?.to_string_lossy().to_string();
    #[cfg(windows)]{
        let auto = AutoLaunch::new(app_name, &app_path, WindowsEnableMode::CurrentUser, &[] as &[&str]);
        Ok(auto)


    }
    #[cfg(target_os = "macos")]{
        let auto = AutoLaunch::new(app_name, &app_path, MacOSLaunchMode::LaunchAgent, &[] as &[&str], &[] as &[&str], "");
        Ok(auto)


    }
    #[cfg(target_os = "linux")]{
        let auto = AutoLaunch::new(app_name, &app_path, LinuxLaunchMode::XdgAutostart, &[] as &[&str]);
        Ok(auto)

    }


}



