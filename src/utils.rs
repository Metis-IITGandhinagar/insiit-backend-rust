use std::{io, thread, time::Duration};
use std::future::Future;

pub async fn save_image(base64_data: &String) -> Result<String, ()> {
    thread::sleep(Duration::from_secs(3));
    Ok(String::from("hi"))
}
