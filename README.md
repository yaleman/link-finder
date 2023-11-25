# link-finder

Yet another web-site-parsing thing that looks for dead links.

The *worker* will treat every URL as distinct, but the backend will strip anchors out (`#blah`) to reduce re-scrapes.

## Usage

This might be out of date, run `link-finder --help` to be sure 😁

```text
Usage: link-finder [OPTIONS] <URL>

Arguments:
  <URL>  The URL to start from

Options:
  -H, --host <HOST>
          Limit the hosts you want to scrape, by default the host in the URL is included
  -i, --include-generic
          If you want to check images and other things we *know* aren't pages.
  -q, --quiet
  -d, --debug
  -m, --max-retries <MAX_RETRIES>
          The maximum amount of times the scraper will loop waiting for a page to process [default: 5]
  -f, --fail-sleep-msecs <FAIL_SLEEP_MSECS>
          The time the scraper will wait for a page to process [default: 250]
  -s, --status-update-secs <STATUS_UPDATE_SECS>
          Number of seconds between status updates from the backend [default: 3]
  -h, --help
          Print help
  -V, --version
          Print version
```

## Process flow

```mermaid


sequenceDiagram

participant Main
participant PageWorker
participant PageStorer
participant Websites

Main->>PageStorer: Start

Main->>Websites: Scrape
Note left of Websites: Grab the first page<br>to start the process.

loop
    PageWorker->>PageStorer: Request More Pages
    Note right of PageStorer: If there's none left<br/>wait <fail_sleep_msecs><br /><max_retries> times
    PageStorer->>PageWorker: Here you Go
    PageStorer->>PageStorer: "Checks out" the page
    PageWorker->>Websites: Enscrapen
    Websites->>PageWorker: Page Response
    loop 
        PageWorker->>PageWorker: Parses the page<br/>making a list of links
    end
    PageWorker->>PageStorer: Got a page, and a bunch of inks!
    Note left of PageStorer: Store the page and check it back in.
    Note left of PageStorer: Store the links.
    PageStorer->>PageWorker: Ta.

end

PageWorker->>Main: We're done!
Main->>PageStorer: Shutdown plz
Note right of Main: We're done!
```
