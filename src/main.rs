use clap::{command, Parser};
use log::{debug, error, info, warn, Level};
use scraper::{Html, Selector};
use serde::Serialize;
use serde_json::json;
use std::collections::HashMap;
use std::fmt::Display;
use std::str::FromStr;
use std::string::FromUtf8Error;
use tokio::sync::{mpsc, oneshot, RwLock};
use url::Url;

static FAIL_SLEEP_MSECS: u64 = 100;
static UPDATE_TIME: u64 = 3;
type PageStore = RwLock<HashMap<LinkType, PageStatus>>;

#[derive(Debug)]
enum LinkFinderError {
    Reqwest(reqwest::Error),
    Status(u16),
    FromUtf8Error(std::string::FromUtf8Error),
}

impl From<reqwest::Error> for LinkFinderError {
    fn from(err: reqwest::Error) -> LinkFinderError {
        LinkFinderError::Reqwest(err)
    }
}

impl From<FromUtf8Error> for LinkFinderError {
    fn from(err: FromUtf8Error) -> LinkFinderError {
        LinkFinderError::FromUtf8Error(err)
    }
}

#[derive(Parser, Debug, Clone)]
#[command(author, version, about)]
struct CliOptions {
    url: Url,
    #[arg(short = 'H', long)]
    host: Vec<String>,
    #[arg(short, long)]
    images: bool,
    #[arg(short, long)]
    quiet: bool,
    #[arg(short, long)]
    json: bool,
}

#[derive(Debug)]
enum StoreRequest {
    /// Get a page we're waiting to process
    GetNextPending(oneshot::Sender<PageStatus>),
    /// Store a page we've just processed
    Store(LinkType, PageStatus),
    /// ensure it's in the list
    Yeet(LinkType),
    ShutDown,
}

#[derive(Clone, Debug)]
enum PageStatus {
    Pending,
    OkImage,
    CheckedOut(u64),
    Ok(String),
    Failed(String),
    NextUrl(LinkType),
    NoUrls,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
enum LinkType {
    Image(Url),
    Page(Url),
}

impl Serialize for LinkType {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            LinkType::Image(url) => serializer.serialize_str(&format!("Image({})", url.as_ref())),
            LinkType::Page(url) => serializer.serialize_str(&format!("Page({})", url.as_ref())),
        }
    }
}

impl Display for LinkType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LinkType::Image(url) => write!(f, "Image: {}", url.as_str()),
            LinkType::Page(url) => write!(f, "Page: {}", url.as_str()),
        }
    }
}

/// Pulls a web page and returns the contents as a String
async fn pull_page(url: Url) -> Result<String, LinkFinderError> {
    debug!("Pulling {}", url.as_str());
    let body = match reqwest::get(url.clone()).await {
        Ok(response) => {
            if response.status().is_success() {
                response.bytes().await?
            } else {
                return Err(LinkFinderError::Status(response.status().as_u16()));
            }
        }
        Err(err) => {
            error!("Failed to pull page: {:?}", err);
            return Err(LinkFinderError::Reqwest(err));
        }
    };
    let body_text = String::from_utf8(body.to_vec())?;
    Ok(body_text)
}

/// Find all the failed ones
async fn get_failed_urls(input: &mut PageStore) -> Vec<(LinkType, String)> {
    input
        .read()
        .await
        .iter()
        .filter_map(|(url, status)| match status {
            PageStatus::Failed(err) => Some((url.clone(), err.clone())),
            _ => None,
        })
        .collect()
}

/// find the next URL to handle
async fn get_pending_from_hashmap(input: &mut PageStore) -> Option<LinkType> {
    input
        .read()
        .await
        .iter()
        .find_map(|(url, status)| match status {
            PageStatus::Pending => Some(url.clone()),
            _ => None,
        })
}

/// get the current epoch time
fn get_checkout_time() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("Somehow I can't get the current time...")
        .as_secs()
}

/// sits around waiting to see if we've got a page to store, and can respond if we've already got it
async fn page_storer(mut rx: mpsc::Receiver<StoreRequest>) -> (usize, Vec<(LinkType, String)>) {
    let mut pages: PageStore = RwLock::new(HashMap::new());
    let start_time = get_checkout_time();
    let mut last_update = get_checkout_time();

    while let Some(store_request) = rx.recv().await {
        if get_checkout_time() - last_update >= UPDATE_TIME {
            last_update = get_checkout_time();
            info!(
                "Running for {} seconds, have seen {} urls",
                (get_checkout_time() - start_time) as u32,
                pages.read().await.len(),
            );
        }
        match store_request {
            StoreRequest::GetNextPending(tx) => {
                if let Some(url) = get_pending_from_hashmap(&mut pages).await {
                    match tx.send(PageStatus::NextUrl(url.clone())) {
                        Ok(_) => {
                            debug!("Checking out {}", &url);
                            let mut writer = pages.write().await;
                            writer.insert(url, PageStatus::CheckedOut(get_checkout_time()));
                        }
                        Err(err) => {
                            error!("Failed to send message back: {:?}", err);
                        }
                    }
                } else if let Err(err) = tx.send(PageStatus::NoUrls) {
                    error!("Failed to send message back: {:?}", err);
                } else {
                    debug!("Couldn't find a URL for worker...");
                }
            }
            StoreRequest::Store(url, pagestatus) => {
                pages.write().await.insert(url, pagestatus);
            }
            StoreRequest::Yeet(url) => {
                let mut writer = pages.write().await;
                if !writer.contains_key(&url) {
                    writer.insert(url.clone(), PageStatus::Pending);
                    info!("New URL: {}", url);
                }
            }
            StoreRequest::ShutDown => {
                info!("Shutting down page_store");
                break;
            }
        }
    }
    let processed_pages = pages.read().await.len();
    let failed_urls = get_failed_urls(&mut pages).await;
    (processed_pages, failed_urls)
}

/// try and turn a  relative URL into an absolute one
fn resolve_relative_url(base_url: Url, new_url: Url) -> Url {
    if new_url.host().is_none() {
        match base_url.join(new_url.as_str()) {
            Ok(url) => url,
            Err(err) => {
                error!("Failed to resolve relative URL: {:?}", err);
                new_url
            }
        }
    } else {
        new_url
    }
}
/// parse a page looking for URLs
fn get_links(base_url: Url, html: &str, hosts: &[String], check_images: bool) -> Vec<LinkType> {
    let a_selector = Selector::parse("a").unwrap();
    let img_selector = Selector::parse("img").unwrap();

    let html = Html::parse_document(html);

    let mut res: Vec<LinkType> = html
        .select(&a_selector)
        .filter_map(|link| {
            let href = link.value().attr("href")?;
            let url = Url::parse(href).ok()?;
            let url = match url.host() {
                None => Some(resolve_relative_url(base_url.clone(), url)),
                Some(_) => Some(url),
            };
            if let Some(url) = url {
                if hosts.contains(&url.host_str().unwrap_or("").to_string()) {
                    Some(LinkType::Page(url))
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();
    if check_images {
        res.extend(html.select(&img_selector).filter_map(|link| {
            let src = link.value().attr("src")?;
            debug!("Found image SRC: {}", src);
            let url = match Url::parse(src) {
                Ok(url) => url,
                Err(err) => {
                    // it's a relative URL, so let's try to smush it on top of the base URL.
                    if let url::ParseError::RelativeUrlWithoutBase = err {
                        match base_url.join(src) {
                            Ok(url) => url,
                            Err(err) => {
                                warn!(
                                    "Failed to rebase img url ({}) on {}, can't handle it: {:?}",
                                    src,
                                    base_url.as_str(),
                                    err
                                );
                                return None;
                            }
                        }
                    } else {
                        warn!("Failed to parse IMG URL, can't handle it: {:?}", err);
                        return None;
                    }
                }
            };

            let url = match url.host() {
                None => Some(resolve_relative_url(base_url.clone(), url)),
                Some(_) => Some(url),
            };
            if let Some(url) = url {
                let host_str = url.host_str().unwrap_or("");

                if hosts.contains(&host_str.to_string()) {
                    Some(LinkType::Image(url))
                } else {
                    warn!(
                        "dropping img as {} not in the host list: {}",
                        host_str,
                        url.as_str()
                    );
                    None
                }
            } else {
                warn!("dropping img as it has no host entry: {:?}", url);
                None
            }
        }));
    }
    debug!("Found {} links", res.len());
    res
}

async fn pull_and_parse_page(url: Url, hosts: &[String], check_images: bool) -> Vec<StoreRequest> {
    match pull_page(url.clone()).await {
        Ok(html) => {
            let links = get_links(url.clone(), &html, hosts, check_images);

            let mut reqs = vec![StoreRequest::Store(
                LinkType::Page(url),
                PageStatus::Ok(html),
            )];

            reqs.extend(links.into_iter().map(StoreRequest::Yeet));
            reqs
        }
        Err(err) => {
            let errstring = format!("{:?}", err);
            error!("Error pulling {}: {}", url.as_str(), errstring);
            vec![StoreRequest::Store(
                LinkType::Page(url),
                PageStatus::Failed(errstring),
            )]
        }
    }
}

const MAX_RETRIES: u8 = 5;

/// this does the thing
async fn page_worker(config: CliOptions, tx: mpsc::Sender<StoreRequest>) {
    let my_id = uuid::Uuid::new_v4();
    for loop_count in 0..MAX_RETRIES {
        loop {
            // get the next URL to process
            let (next_url_tx, next_url_rx) = oneshot::channel();
            let gnp = tx.send(StoreRequest::GetNextPending(next_url_tx)).await;
            if gnp.is_err() {
                error!("{} Failed to send message to store: {:?}", my_id, gnp);
                break;
            }

            let response = next_url_rx.await.unwrap();

            match response {
                PageStatus::Pending
                | PageStatus::CheckedOut(_)
                | PageStatus::Ok(_)
                | PageStatus::OkImage
                | PageStatus::Failed(_) => {
                    error!(
                        "Got weird message when asking for NextPending: {:?}",
                        response
                    );
                    break;
                }
                PageStatus::NextUrl(url) => {
                    let store_requests: Vec<StoreRequest> = match url.clone() {
                        LinkType::Image(img_url) => {
                            match reqwest::Client::new().head(img_url.clone()).send().await {
                                Ok(res) => {
                                    if res.status().is_success() {
                                        vec![StoreRequest::Store(url, PageStatus::OkImage)]
                                    } else {
                                        warn!("Failed to HEAD image {}", img_url.as_ref());
                                        vec![StoreRequest::Store(
                                            url,
                                            PageStatus::Failed(format!(
                                                "{:?}",
                                                LinkFinderError::Status(res.status().as_u16())
                                            )),
                                        )]
                                    }
                                }
                                Err(err) => vec![StoreRequest::Store(
                                    url,
                                    PageStatus::Failed(format!("{:?}", err)),
                                )],
                            }
                        }
                        LinkType::Page(url) => {
                            pull_and_parse_page(url, &config.host, config.images).await
                        }
                    };

                    let mut errors: u8 = 0;
                    for req in store_requests {
                        if let Err(err) = tx.send(req).await {
                            error!("{} Failed to send message to store: {:?}", my_id, err);
                            // tokio::time::sleep(std::time::Duration::from_secs(FAIL_SLEEP_SECS)).await;
                            errors = 1
                        }
                    }
                    if errors > 0 {
                        break;
                    }
                }
                PageStatus::NoUrls => {
                    debug!(
                        "{} waiting on URL: remaining attempts: {}",
                        my_id,
                        MAX_RETRIES - loop_count
                    );
                    break;
                }
            }
        }
        tokio::time::sleep(std::time::Duration::from_micros(FAIL_SLEEP_MSECS)).await;
    }
}

#[tokio::main]
async fn main() {
    let mut options = CliOptions::parse();

    let default_level = if options.quiet {
        Level::Warn
    } else {
        Level::Info
    };

    let level = match std::env::var("RUST_LOG") {
        Ok(val) => Level::from_str(&val).unwrap_or(default_level),
        Err(_) => default_level,
    };
    simple_logger::init_with_level(level).expect("Failed to start logger!");

    let target_url_host = options
        .url
        .host()
        .unwrap_or(url::Host::Domain(""))
        .to_string();

    if target_url_host.is_empty() {
        error!("Couldn't get host from URL {}", options.url.as_str());
        std::process::exit(1);
    }
    if !options.host.contains(&target_url_host) {
        options.host.push(target_url_host.clone());
    }

    let (tx, rx) = mpsc::channel(100);

    // do the first URL, for good justice.
    info!("Checking {}", options.url);
    let res = pull_and_parse_page(options.url.clone(), &options.host, options.images).await;
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

    let store_process = tokio::spawn(page_storer(rx));

    // spawn cpus-2 workers
    // get the number of CPUs we have
    let cpus = num_cpus::get();

    // let worker = page_worker(tx.clone());
    let mut workers = Vec::new();

    (0..cpus - 2).for_each(|_| {
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
        Ok((num_urls, errors)) => {
            info!("Store process exited: processed {:?} urls", num_urls);
            if !errors.is_empty() {
                error!("Failed URLs!");
                errors.iter().for_each(|(url, err)| {
                    if options.json {
                        println!(
                            "{}",
                            serde_json::to_string(&json!({"url": url, "error": err})).unwrap()
                        );
                    } else {
                        error!("{}: {}", url, err);
                    }
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
