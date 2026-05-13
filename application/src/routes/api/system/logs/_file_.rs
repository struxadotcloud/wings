use super::State;
use utoipa_axum::{router::OpenApiRouter, routes};

mod get {
    use crate::{
        io::compression::reader::AsyncCompressionReader,
        response::{ApiResponse, ApiResponseResult},
        routes::{ApiError, GetState},
    };
    use axum::extract::{Path, Query};
    use serde::Deserialize;
    use tokio::io::AsyncRead;
    use utoipa::ToSchema;

    #[derive(ToSchema, Deserialize)]
    pub struct Params {
        lines: Option<usize>,
    }

    #[utoipa::path(get, path = "/", responses(
        (status = OK, body = String),
        (status = NOT_FOUND, body = ApiError)
    ), params(
        (
            "file" = String,
            description = "The log file name",
            example = "wings.log",
        ),
        (
            "lines" = Option<usize>, Query,
            description = "The number of lines to tail from the log file",
            example = "100",
        ),
    ))]
    pub async fn route(
        state: GetState,
        Path(file_path): Path<compact_str::CompactString>,
        Query(params): Query<Params>,
    ) -> ApiResponseResult {
        if file_path.contains("..") {
            return ApiResponse::error("log file not found").ok();
        }

        let mut file = match tokio::fs::File::open(
            std::path::Path::new(&state.config.load().system.log_directory).join(&file_path),
        )
        .await
        {
            Ok(file) => file,
            Err(_) => return ApiResponse::error("log file not found").ok(),
        };

        let reader: Box<dyn AsyncRead + Send + Unpin> = if file_path.ends_with(".gz") {
            let gz_reader = AsyncCompressionReader::new_mt(
                file.into_std().await,
                crate::io::compression::CompressionType::Gz,
                state.config.load().api.file_decompression_threads,
            );

            if let Some(lines) = params.lines {
                Box::new(crate::io::tail::async_tail_stream(gz_reader, lines).await?)
            } else {
                Box::new(gz_reader)
            }
        } else {
            if let Some(lines) = params.lines {
                file = crate::io::tail::async_tail(file, lines).await?;
            }

            Box::new(file)
        };

        ApiResponse::new_stream(reader)
            .with_header("Content-Type", "text/plain")
            .ok()
    }
}

pub fn router(state: &State) -> OpenApiRouter<State> {
    OpenApiRouter::new()
        .routes(routes!(get::route))
        .with_state(state.clone())
}
