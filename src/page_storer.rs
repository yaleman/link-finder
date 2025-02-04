use std::collections::HashMap;

use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use log::*;
use tokio::sync::mpsc;

use crate::{CliOptions, LinkType, PageStatus, StoreRequest};

type PageStore = RwLock<HashMap<LinkType, PageStatus>>;

fn get_storable_link(url: &LinkType) -> LinkType {
    let mut actual_url = match url {
        LinkType::Generic(url) => url,
        LinkType::Page(url) => url,
    }
    .to_owned();
    actual_url.set_fragment(None);

    url.with_url(actual_url)
}

/// This adds it to the internal data store, but strips fragments to reduce re-scraping (`#fragment`)
async fn add_link(
    store: &mut RwLockWriteGuard<'_, HashMap<LinkType, PageStatus>>,
    url: LinkType,
    status: PageStatus,
) {
    let url: LinkType = get_storable_link(&url);
    debug!("Storing {} -> {}", &url, status.as_name());
    store.insert(url, status);
}

async fn has_checkouts(input: &RwLockReadGuard<'_, HashMap<LinkType, PageStatus>>) -> usize {
    input
        .iter()
        .filter_map(|(_, status)| {
            if let PageStatus::CheckedOut(_) = status {
                Some(1)
            } else {
                None
            }
        })
        .collect::<Vec<usize>>()
        .len()
}

/// Figure out if we have another URL, or if there's some checkouts, or if we're done
async fn get_next_url(input: &mut PageStore) -> PageStatus {
    let reader = input.read().await;
    match reader.iter().find_map(|(url, status)| match status {
        PageStatus::Pending => Some(PageStatus::NextUrl(url.clone())),
        _ => None,
    }) {
        Some(val) => val,
        None => {
            let has_checkouts = has_checkouts(&reader).await;
            if has_checkouts > 0 {
                PageStatus::HasCheckouts(has_checkouts)
            } else {
                PageStatus::NoPendingUrls
            }
        }
    }
}

/// get the current epoch time
fn get_checkout_time() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("Somehow I can't get the current time...")
        .as_secs()
}

pub struct PageStorerResults {
    pub processed_pages: usize,
    pub failed_urls: Vec<(LinkType, String)>,
    pub ok_urls: Vec<LinkType>,
}

/// This is the worker that sits around waiting to see if we've got a page to store, and can respond if we've already got it
pub async fn page_storer(
    config: CliOptions,
    mut rx: mpsc::Receiver<StoreRequest>,
) -> PageStorerResults {
    let mut pages: PageStore = RwLock::new(HashMap::new());
    let start_time = get_checkout_time();
    let mut last_update = get_checkout_time();

    while let Some(store_request) = rx.recv().await {
        if get_checkout_time() - last_update >= config.status_update_secs {
            last_update = get_checkout_time();
            info!(
                "Running for {} seconds, have seen {} urls",
                (get_checkout_time() - start_time) as u32,
                pages.read().await.len(),
            );
        }
        match store_request {
            StoreRequest::GetNextPending(tx) => {
                let next_url = get_next_url(&mut pages).await;
                let url = match &next_url {
                    PageStatus::NextUrl(url) => Some(url.clone()),
                    PageStatus::HasCheckouts(_)
                    | PageStatus::Pending
                    | PageStatus::OkGeneric
                    | PageStatus::CheckedOut(_)
                    | PageStatus::Ok(_)
                    | PageStatus::Failed(_)
                    | PageStatus::NoPendingUrls => None,
                };

                match tx.send(next_url) {
                    Ok(_) => {
                        if let Some(url) = url {
                            debug!("Checking out {}", &url);
                            let mut writer = pages.write().await;
                            add_link(
                                &mut writer,
                                url,
                                PageStatus::CheckedOut(get_checkout_time()),
                            )
                            .await;
                        }
                    }
                    Err(err) => {
                        error!("Failed to send message back: {:?}", err);
                    }
                }
            }
            StoreRequest::Store(url, pagestatus) => {
                let mut writer = pages.write().await;
                add_link(&mut writer, url, pagestatus).await;
            }
            StoreRequest::Yeet(url) => {
                let mut writer = pages.write().await;
                if !writer.contains_key(&get_storable_link(&url)) {
                    add_link(&mut writer, url.clone(), PageStatus::Pending).await;
                    debug!("page_storer - storing new URL: {}", url);
                }
            }
            StoreRequest::ShutDown => {
                debug!("Shutting down page_store");
                break;
            }
        }
    }
    let processed_pages = pages.read().await.len();
    let failed_urls = get_failed_urls(&mut pages).await;

    PageStorerResults {
        processed_pages,
        failed_urls,
        ok_urls: get_ok_urls(&mut pages).await,
    }
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

/// Find all the failed ones
async fn get_ok_urls(input: &mut PageStore) -> Vec<LinkType> {
    input
        .read()
        .await
        .iter()
        .filter_map(|(url, status)| match *status {
            PageStatus::Ok(_) | PageStatus::OkGeneric => Some(url.clone()),
            PageStatus::Failed(_)
            | PageStatus::HasCheckouts(_)
            | PageStatus::Pending
            | PageStatus::CheckedOut(_)
            | PageStatus::NextUrl(_)
            | PageStatus::NoPendingUrls => None,
        })
        .collect()
}
