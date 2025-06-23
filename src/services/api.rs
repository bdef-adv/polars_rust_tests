use std::time::Instant;

use actix_web::http::header::ContentType;
use actix_web::{get, web, HttpResponse, Responder, post};
use actix_web::web::Json;

use crate::logger_elapsed;
use crate::feed::{get_feed,new_temporary_feed};
use crate::feed::parameters::FeedParameters;
use crate::io::lazyframe::{lazyframe_as_str,lazyframe_as_arrowbytes};


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
}


#[post("/feeds")]
pub async fn new_temp_feed(params: Json<FeedParameters>) -> impl Responder {
    const LOG_HEADER: &str = "services::new_temp_feed";

    // Init timer
    let timer = Instant::now();

    let feed_name = params.feed_name.clone();

    logger_elapsed!(timer, "{LOG_HEADER} Fetching feed {feed_name}");

    match new_temporary_feed(&feed_name, &params) {
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
}


#[get("/feeds/{feed_name}")]
pub async fn download_feed(path: web::Path<String>) -> impl Responder {
    const LOG_HEADER: &str = "services::download_feed";

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
            match lazyframe_as_arrowbytes(lf) {
                Ok(arrow_bytes) => {
                    logger_elapsed!(timer, "{LOG_HEADER} Collected feed {feed_name}");
                    HttpResponse::Ok()
                        .content_type("application/vnd.apache.arrow.file")
                        .append_header(("Content-Disposition", "attachment; filename=\"data.arrow\""))
                        .body(arrow_bytes)
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
}
