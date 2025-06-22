use std::time::Instant;

use actix_web::http::header::ContentType;
use actix_web::{get, web, HttpResponse, Responder};

use crate::logger_elapsed;
use crate::data::{get_feed,lazyframe_as_str};


#[get("/feeds/{feed_name}/display")]
pub async fn display_feed(path: web::Path<String>) -> impl Responder {
    const LOG_HEADER: &str = "services::display_feed";

    // Path variables
    let feed_name = path.into_inner();

    // Init timer
    let timer = Instant::now();

    logger_elapsed!(timer, "{LOG_HEADER} Fetching feed {feed_name}");

    match get_feed(&feed_name) {
        // Feed has been properly defined as a LazyFrame
        Ok(lf) => {
            logger_elapsed!(timer, "{LOG_HEADER} Fetched feed {feed_name}");

            match lf.explain(true) {
                Ok(lf_explained) => {
                    logger_elapsed!(timer, "{LOG_HEADER} {lf_explained}");
                },
                Err(err) => {
                    logger_elapsed!(timer, "{LOG_HEADER} Error while explaining feed {feed_name}: {err}");
                    return HttpResponse::InternalServerError()
                        .content_type(ContentType::plaintext())
                        .body(format!("Error while explaining feed {feed_name}: {err}"))
                }
            }

            // Convert LazyFrame to DataFrame and display it
            match lazyframe_as_str(lf) {
                Ok(result) => {
                    logger_elapsed!(timer, "{LOG_HEADER} Collected feed {feed_name}");
                    HttpResponse::Ok()
                        .content_type(ContentType::plaintext())
                        .body(result)
                },
                Err(err) => {
                    logger_elapsed!(timer, "{LOG_HEADER} Error while collecting feed {feed_name}: {err}");
                    HttpResponse::InternalServerError()
                        .content_type(ContentType::plaintext())
                        .body(format!("Error while collecting feed {feed_name}: {err}"))
                }
            }
        },
        Err(err) => {
            logger_elapsed!(timer, "{LOG_HEADER} Error while fetching feed {feed_name}: {err}");
            HttpResponse::InternalServerError()
                .content_type(ContentType::plaintext())
                .body(format!("Error while fetching feed {feed_name}: {err}"))
        }
    }

    /*let feed_a = load_lazyframe_from_ipc("arrow_data/data_a.arrow").unwrap();
    let feed_b = load_lazyframe_from_ipc("arrow_data/data_b.arrow_100000000").unwrap();
    let feed_c = load_lazyframe_from_ipc("arrow_data/data_b.arrow_300000000").unwrap();
    logger_elapsed!(timer, "Initialized LazyFrames:");

    let feed_d = join_timestamp_value(feed_a, feed_b);
    let feed_e = operation_b(feed_d, feed_c);

    display_lazyframe(feed_e).unwrap();

    logger_elapsed!(timer, "Elapsed:");*/
}