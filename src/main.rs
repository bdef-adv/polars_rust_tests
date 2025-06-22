use actix_web::{App, HttpServer};
use actix_cors::Cors;

mod utils;
mod data;
mod operations;
mod services;

use services::api::display_feed;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    HttpServer::new(|| {
        let cors = Cors::permissive();
        App::new()
            .wrap(cors)
            .service(display_feed)
    })
    .bind(("0.0.0.0".to_string(), 3000))?
    .run()
    .await
}
