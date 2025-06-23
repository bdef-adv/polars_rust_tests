use actix_web::{App, HttpServer};
use actix_cors::Cors;

mod utils;
mod data;
mod operations;
mod services;
mod filters;
mod fs;
mod io;

use services::api::{display_feed,download_feed};

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;


#[actix_web::main]
async fn main() -> std::io::Result<()> {
    HttpServer::new(|| {
        let cors = Cors::permissive();
        App::new()
            .wrap(cors)
            .service(display_feed)
            .service(download_feed)
    })
    .bind(("0.0.0.0".to_string(), 3000))?
    .run()
    .await
}
