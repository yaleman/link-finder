use clap::Parser;
use link_finder::page_storer::page_storer;
use link_finder::page_worker::{page_worker, pull_and_parse_page};
use link_finder::{CliOptions, StoreRequest};
use log::{error, info};
use serde_json::json;
use structured_logger::{async_json::new_writer, Builder};
use tokio::sync::mpsc;

fn setup_logging(level: &str) {
    Builder::with_level(level)
        .with_target_writer("*", new_writer(tokio::io::stderr()))
        .init();
}

#[tokio::main]
async fn main() {
    let mut options = CliOptions::parse();

    let level = if options.quiet { "warn" } else { "info" };
    let level = if options.debug { "debug" } else { level };
    setup_logging(level);

    let target_url_host = options
        .url
        .host()
        .unwrap_or(url::Host::Domain(""))
        .to_string();

    if target_url_host.is_empty() {
        error!(
            "Couldn't get host from URL, can't work with this: {}",
            options.url.as_str()
        );
        std::process::exit(1);
    }
    if !options.host.contains(&target_url_host) {
        options.host.push(target_url_host.clone());
    }

    let (tx, rx) = mpsc::channel(100);

    // do the first URL, to start the process off.
    let res =
        pull_and_parse_page(options.url.clone(), &options.host, options.include_generic).await;
    let mut bail = false;
    for req in res {
        if let Err(err) = tx.send(req).await {
            error!("Failed to pull first URL: {:?}", err);
            bail = true;
        }
    }
    if bail {
        error!("Quitting!");
        return;
    } else {
        info!("Pulled first URL, starting workers")
    }

    let store_process = tokio::spawn(page_storer(options.clone(), rx));

    // spawn cpus-2 workers
    // get the number of CPUs we have
    let cpus = num_cpus::get();

    let mut workers = Vec::new();
    (0..cpus - 1).for_each(|_| {
        let my_tx = tx.clone();
        workers.push(page_worker(options.clone(), my_tx));
    });

    for worker in workers {
        worker.await;
    }

    if let Err(err) = tx.send(StoreRequest::ShutDown).await {
        error!("Well, we failed to send the SHUTDOWN message. {:?}", err);
        return;
    }

    match store_process.await {
        Ok(results) => {
            info!(
                "Store process finished: processed {:?} urls",
                &results.processed_pages
            );

            if !options.only_show_errors {
                results.ok_urls.iter().for_each(|url| {
                    let logline = json!({
                        "status" : "ok",
                        "url": &url
                    });
                    println!(
                        "{}",
                        serde_json::to_string(&logline).unwrap_or_else(|err| format!(
                            "Failed to serialize {:?}: {:?}",
                            url, err
                        ))
                    );
                })
            }

            if !results.failed_urls.is_empty() {
                // error!("Failed URLs!");
                results.failed_urls.iter().for_each(|(url, err)| {
                    let logline = json!({
                        "status" : "failed",
                        "url": url,
                        "error": err
                    });
                    println!(
                        "{}",
                        serde_json::to_string(&logline).unwrap_or_else(|err| format!(
                            "Failed to serialize {:?}: {:?}",
                            logline, err
                        ))
                    );
                });
                std::process::exit(1);
            }
        }
        Err(err) => {
            error!("Failed to finish store process: {:?}", err);
            std::process::exit(1);
        }
    };
}
