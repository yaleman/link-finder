pub mod page_storer;
pub mod page_worker;

use clap::{command, Parser};

use std::fmt::Display;

use serde::Serialize;
use tokio::sync::oneshot;
use url::Url;

#[derive(Parser, Debug, Clone)]
#[command(author, version, about)]
pub struct CliOptions {
    /// The URL to start from
    pub url: Url,
    #[arg(short = 'H', long)]
    /// Limit the hosts you want to scrape, by default the host in the URL is included.
    pub host: Vec<String>,
    #[arg(short, long)]
    /// If you want to check images and other things we *know* aren't pages
    pub include_generic: bool,
    #[arg(short, long)]
    pub quiet: bool,
    #[arg(short, long)]
    pub debug: bool,
    #[arg(short, long, default_value = "5")]
    /// The maximum amount of times the scraper will loop waiting for a page to process
    pub max_retries: u8,
    #[arg(short, long, default_value = "250")]
    /// The time (in milliseconds) the scraper will wait for a page to process
    pub fail_sleep_msecs: u64,
    #[arg(short, long, default_value = "3")]
    /// Number of seconds between status updates from the backend
    pub status_update_secs: u64,
    // TODO: work out how to do this
    // #[arg(long, default_value = "false")]
    // If the scraper should ignore different ports on the same host
    // pub ignore_different_ports: bool,
    #[arg(
        short = 'e',
        long,
        default_value = "false",
        // action = clap::ArgAction::SetFalse
    )]
    pub only_show_errors: bool,
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
    OkGeneric,
    CheckedOut(u64),
    Ok(String),
    Failed(String),
    NextUrl(LinkType),
    NoPendingUrls,
    HasCheckouts(usize),
}

impl PageStatus {
    pub fn as_name(&self) -> &'static str {
        match self {
            PageStatus::Pending => "Pending",
            PageStatus::OkGeneric => "OkGeneric",
            PageStatus::CheckedOut(_) => "CheckedOut",
            PageStatus::Ok(_) => "Ok",
            PageStatus::Failed(_) => "Failed",
            PageStatus::NextUrl(_) => "NextUrl",
            PageStatus::NoPendingUrls => "NoPendingUrls",
            PageStatus::HasCheckouts(_) => "HasCheckouts",
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum LinkType {
    Generic(Url),
    Page(Url),
}

impl LinkType {
    pub fn with_url(&self, new_url: Url) -> Self {
        match self {
            LinkType::Generic(_) => Self::Generic(new_url.clone()),
            LinkType::Page(_) => Self::Page(new_url.clone()),
        }
    }
}

impl Serialize for LinkType {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            LinkType::Generic(url) => {
                serializer.serialize_str(&format!("Generic({})", url.as_ref()))
            }
            LinkType::Page(url) => serializer.serialize_str(&format!("Page({})", url.as_ref())),
        }
    }
}

impl Display for LinkType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LinkType::Generic(url) => write!(f, "Generic: {}", url.as_str()),
            LinkType::Page(url) => write!(f, "Page: {}", url.as_str()),
        }
    }
}
