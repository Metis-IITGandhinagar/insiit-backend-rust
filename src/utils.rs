use base64::{ engine::general_purpose::STANDARD as BASE64, Engine };
use std::sync::atomic::{ AtomicU64, Ordering };
use time::OffsetDateTime;
use tokio::io::AsyncWriteExt;

static IMAGE_COUNTER: AtomicU64 = AtomicU64::new(0);

pub async fn save_image(base64_data: &String, image_directory_path: &String) -> Result<String, ()> {
    let clean_base64 = if let Some(pos) = base64_data.find(",") {
        &base64_data[pos + 1..]
    } else {
        base64_data
    };
    let image_bytes = match BASE64.decode(clean_base64) {
        Ok(bytes) => bytes,
        Err(_) => return Err(()),
    };
    let unique = IMAGE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let timestamp = OffsetDateTime::now_utc().unix_timestamp_nanos();
    let filename = format!("{timestamp}-{unique}");
    let mut file = match tokio::fs::File::create(format!("{image_directory_path}/{filename}")).await {
        Ok(file) => file,
        Err(_) => return Err(()),
    };
    match file.write_all(&image_bytes).await {
        Ok(_) => Ok(format!("images/{filename}")),
        Err(_) => Err(())
    }
}
