use crate::{CliOptions, LinkType, PageStatus, StoreRequest};
use log::*;
use scraper::{Html, Selector};
use std::string::FromUtf8Error;
use tokio::sync::{mpsc, oneshot};
use url::{ParseError, Url};

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

/// Page worker is the one doing the page access
pub async fn page_worker(config: CliOptions, tx: mpsc::Sender<StoreRequest>) {
    let my_id = uuid::Uuid::new_v4();
    for loop_count in 0..config.max_retries {
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
                | PageStatus::OkGeneric
                | PageStatus::Failed(_) => {
                    error!(
                        "Got weird message when asking for NextPending: {:?}",
                        response
                    );
                    break;
                }
                PageStatus::NextUrl(url) => {
                    let store_requests: Vec<StoreRequest> = match url.clone() {
                        LinkType::Generic(img_url) => {
                            match reqwest::Client::new().head(img_url.clone()).send().await {
                                Ok(res) => {
                                    if res.status().is_success() {
                                        vec![StoreRequest::Store(url, PageStatus::OkGeneric)]
                                    } else {
                                        debug!("Failed to HEAD generic item {}", img_url.as_ref());
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
                            pull_and_parse_page(url, &config.host, config.include_generic).await
                        }
                    };

                    let mut errors: u8 = 0;
                    for req in store_requests {
                        if let Err(err) = tx.send(req).await {
                            error!("{} Failed to send message to store: {:?}", my_id, err);
                            errors = 1
                        }
                    }
                    if errors > 0 {
                        break;
                    }
                }
                PageStatus::HasCheckouts(count) => {
                    debug!(
                        "{} waiting on URL: no URLs, but {} checked out. Sleeping for {}ms",
                        my_id, count, &config.fail_sleep_msecs
                    );
                    tokio::time::sleep(std::time::Duration::from_micros(config.fail_sleep_msecs))
                        .await;
                }
                PageStatus::NoPendingUrls => {
                    debug!(
                        "{} waiting on URL: remaining attempts: {}",
                        my_id,
                        config.max_retries - loop_count
                    );
                    break;
                }
            }
        }
        tokio::time::sleep(std::time::Duration::from_micros(config.fail_sleep_msecs)).await;
    }
}

const GENERIC_EXTENSIONS: [&str; 13] = [
    "pdf", "txt", "tif", "xml", "jpg", "jpeg", "png", "gif", "webp", "xml", "zip", "7z", "rar",
];

/// parse a page looking for URLs
fn get_links(base_url: Url, html: &str, hosts: &[String], check_generic: bool) -> Vec<LinkType> {
    let html = Html::parse_document(html);

    let mut res: Vec<LinkType> = html
        .select(&Selector::parse("a").expect("Failed to get selector"))
        .filter_map(|link| {
            let href = link.value().attr("href")?;
            let url = match Url::parse(href) {
                Ok(url) => url,
                Err(err) => match handle_parseerror(href, &base_url, err) {
                    Some(url) => url,
                    None => return None,
                },
            };

            if hosts.contains(&url.host_str().unwrap_or("").to_string()) {
                // lazy check to see if we've found a link to an image etc
                let path = url.path().to_lowercase();
                let ext = path.split('.').last().unwrap_or("");
                if GENERIC_EXTENSIONS.contains(&ext) {
                    debug!("Found generic link: {}", url.as_str());
                    if check_generic {
                        let link = LinkType::Generic(url);
                        debug!("Found {}", &link);
                        Some(link)
                    } else {
                        debug!("Dropping {} as not including generics", url.as_str());
                        None
                    }
                } else {
                    let link = LinkType::Page(url);
                    debug!("Found {}", &link);
                    Some(link)
                }
            } else {
                debug!("Dropping {} as doesn't match host list", url.as_str());
                None
            }
        })
        .collect();
    if check_generic {
        res.extend(check_for_images(&html, &base_url, hosts));
        res.extend(check_for_linkrel(&html, &base_url, hosts));
        res.extend(check_for_scripts(&html, &base_url, hosts));
    }
    debug!("Found {} links", res.len());
    res
}

fn check_for_generic(
    html: &Html,
    base_url: &Url,
    hosts: &[String],
    selector: &Selector,
    attribute: &str,
) -> Vec<LinkType> {
    html.select(selector)
        .filter_map(|link| {
            let src = link.value().attr(attribute)?;
            debug!("Found generic SRC: {}", src);
            let url = match Url::parse(src) {
                Ok(url) => url,
                Err(err) => match handle_parseerror(src, base_url, err) {
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
                    Some(LinkType::Generic(url))
                } else {
                    debug!(
                        "dropping generic as {} not in the host list: {}",
                        host_str,
                        url.as_str()
                    );
                    None
                }
            } else {
                debug!("dropping generic link as it has no host entry: {:?}", url);
                None
            }
        })
        .collect()
}

fn check_for_scripts(html: &Html, base_url: &Url, hosts: &[String]) -> Vec<LinkType> {
    check_for_generic(
        html,
        base_url,
        hosts,
        &Selector::parse("script").expect("Couldn't parse selector"),
        "src",
    )
}

fn check_for_linkrel(html: &Html, base_url: &Url, hosts: &[String]) -> Vec<LinkType> {
    check_for_generic(
        html,
        base_url,
        hosts,
        &Selector::parse("link").expect("Couldn't parse selector"),
        "href",
    )
}

fn check_for_images(html: &Html, base_url: &Url, hosts: &[String]) -> Vec<LinkType> {
    check_for_generic(
        html,
        base_url,
        hosts,
        &Selector::parse("img").expect("Failed to get selector"),
        "src",
    )
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
            debug!("Error pulling {}: {}", url.as_str(), errstring);
            vec![StoreRequest::Store(
                LinkType::Page(url),
                PageStatus::Failed(errstring),
            )]
        }
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
        None
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
            debug!("Failed to pull page: {:?}", err);
            return Err(LinkFinderError::Reqwest(err));
        }
    };
    Ok(body)
}

#[test]
fn test_get_links() {
    let base_url = Url::parse("http://example.com").unwrap();
    let hosts = vec!["example.com".to_string(), "www.iana.org".to_string()];
    let html = include_str!("../tests/example.com.html");
    let links = get_links(base_url, html, &hosts, true);
    println!("{:?}", links);
    assert_eq!(links.len(), 1);
}
