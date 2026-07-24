use base64::{ engine::general_purpose::STANDARD as BASE64, Engine };
use time::OffsetDateTime;
use std::{ fs::File, io, io::Write, thread, time::Duration};
use std::future::Future;

pub async fn save_image(base64_data: &String, image_directory_path: &String) -> Result<String, ()> {
    // I don't really know how robust this is, but a if couple or more unit tests are written and
    // they work properly, I don't mind this implementation
    let clean_base64 = if let Some(pos) = base64_data.find(",") {
        &base64_data[pos + 1..]
    } else {
        base64_data
    };
    let image_bytes = BASE64.decode(clean_base64);
    let image_bytes = if image_bytes.is_ok() {
        image_bytes.unwrap()
    } else {
        return Err(());
    };
    let filename = OffsetDateTime::now_utc().unix_timestamp().to_string();
    let mut file = if let Ok(file) = File::create(format!("{image_directory_path}/{filename}")) {
        file
    } else {
        return Err(());
    };
    match file.write_all(&image_bytes) {
        Ok(_) => Ok(format!("images/{filename}")),
        Err(_) => Err(())
    }
}
