use clap::{command, Parser};

use std::collections::HashMap;
use std::fmt::Display;
use std::string::FromUtf8Error;

use log::{debug, error, info, warn};
use scraper::{Html, Selector};
use serde::Serialize;
use tokio::sync::{mpsc, oneshot, RwLock};
use url::{ParseError, Url};

static MAX_RETRIES: u8 = 5;
static FAIL_SLEEP_MSECS: u64 = 100;
static UPDATE_TIME: u64 = 3;
type PageStore = RwLock<HashMap<LinkType, PageStatus>>;

#[derive(Parser, Debug, Clone)]
#[command(author, version, about)]
pub struct CliOptions {
    pub url: Url,
    #[arg(short = 'H', long)]
    pub host: Vec<String>,
    #[arg(short, long)]
    pub images: bool,
    #[arg(short, long)]
    pub quiet: bool,
    #[arg(short, long)]
    pub debug: bool,
}

#[derive(Debug)]
pub enum LinkFinderError {
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

#[derive(Debug)]
pub enum StoreRequest {
    /// Get a page we're waiting to process
    GetNextPending(oneshot::Sender<PageStatus>),
    /// Store a page we've just processed
    Store(LinkType, PageStatus),
    /// ensure it's in the list
    Yeet(LinkType),
    ShutDown,
}

#[derive(Clone, Debug)]
pub enum PageStatus {
    Pending,
    OkImage,
    CheckedOut(u64),
    Ok(String),
    Failed(String),
    NextUrl(LinkType),
    NoUrls,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum LinkType {
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
                response.text().await?
            } else {
                return Err(LinkFinderError::Status(response.status().as_u16()));
            }
        }
        Err(err) => {
            error!("Failed to pull page: {:?}", err);
            return Err(LinkFinderError::Reqwest(err));
        }
    };
    Ok(body)
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
pub async fn page_storer(mut rx: mpsc::Receiver<StoreRequest>) -> (usize, Vec<(LinkType, String)>) {
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
                    debug!("page_storer - storing new URL: {}", url);
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

fn handle_parseerror(href: &str, base_url: &Url, err: ParseError) -> Option<Url> {
    // it's a relative URL, so let's try to smush it on top of the base URL.
    if let url::ParseError::RelativeUrlWithoutBase = err {
        match base_url.join(href) {
            Ok(url) => Some(url),
            Err(err) => {
                warn!(
                    "Failed to rebase link url ({}) on {}, can't handle it: {:?}",
                    href,
                    base_url.as_str(),
                    err
                );
                None
            }
        }
    } else {
        warn!("Failed to parse link URL, can't handle it: {:?}", err);
        return None;
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
            let url = match Url::parse(href) {
                Ok(url) => url,
                Err(err) => match handle_parseerror(href, &base_url, err) {
                    Some(url) => url,
                    None => return None,
                },
            };
            info!("Found link: {}", url.as_ref());

            if hosts.contains(&url.host_str().unwrap_or("").to_string()) {
                Some(LinkType::Page(url))
            } else {
                debug!("Dropping {} as doesn't match host list", url.as_str());
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
                Err(err) => match handle_parseerror(src, &base_url, err) {
                    Some(url) => url,
                    None => return None,
                },
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

pub async fn pull_and_parse_page(
    url: Url,
    hosts: &[String],
    check_images: bool,
) -> Vec<StoreRequest> {
    debug!("Pulling {}", url.as_str());
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

/// this does the thing
pub async fn page_worker(config: CliOptions, tx: mpsc::Sender<StoreRequest>) {
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
