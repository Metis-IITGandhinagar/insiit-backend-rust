use sqlx::{ postgres::PgPool, query };

pub async fn initialize_database(pool: &PgPool) -> Result<(), String> {
    let init_query = sqlx::query("CREATE TABLE IF NOT EXISTS mess(
            day varchar(20)
    )");
    match init_query.execute(pool).await {
        Ok(_) => {},
        Err(e) => return Err(format!("Couldn't initialize mess_table: {e}"))
    };
    Ok(())
}
