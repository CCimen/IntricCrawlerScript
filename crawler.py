import os
import logging
import time
import sys
import signal
from argparse import ArgumentParser
from dataclasses import dataclass
from typing import List, Dict, Set, Optional
from datetime import datetime, timedelta

import requests
from dotenv import load_dotenv
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ThreadPoolExecutor

# ANSI color codes for console output
RED = "\033[91m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
BLUE = "\033[94m"
MAGENTA = "\033[95m"
CYAN = "\033[96m"
RESET = "\033[0m"

# Configure logging (file + console)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("crawler.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class AppConfig:
    """Application configuration container"""
    api_key: str
    base_url: str
    schedule_minutes: int
    website_filter: Set[str]
    test_mode: bool
    space_id: Optional[str] = None
    space_name: Optional[str] = None
    status_check_interval: int = 60  # Seconds between status checks

class CrawlerAPIClient:
    """Handles API communication with enhanced error handling"""
    
    def __init__(self, config: AppConfig):
        self.config = config
        self.session = requests.Session()
        self.session.headers.update({
            "api-key": self.config.api_key,
            "accept": "application/json"
        })
        self.session.mount('https://', requests.adapters.HTTPAdapter(
            max_retries=3,
            pool_maxsize=10
        ))

    def _handle_api_error(self, response: requests.Response):
        """Handle API errors with detailed logging"""
        try:
            error_data = response.json()
        except ValueError:
            error_data = {"detail": "Unknown error - non-JSON response"}

        logger.error(f"{RED}API Error {response.status_code}: {error_data.get('detail', 'Unknown error')}{RESET}")
        logger.debug(f"Error response headers: {response.headers}")
        logger.debug(f"Error response content: {response.text[:1000]}")

        if response.status_code == 422:
            raise ValueError(f"Validation error: {error_data.get('detail', 'Check request parameters')}")
        response.raise_for_status()

    def _normalize_url(self, url: Optional[str]) -> str:
        """Normalize URLs for consistent comparison"""
        if not url:
            return ""
        return url.lower().strip().rstrip('/')

    def get_spaces(self) -> List[Dict]:
        """
        Fetch all spaces the user can access.
        GET /api/v1/spaces/
        """
        try:
            logger.info(f"{CYAN}Fetching all spaces from {self.config.base_url}/spaces/{RESET}")
            response = self.session.get(f"{self.config.base_url}/spaces/", timeout=10)
            if not response.ok:
                self._handle_api_error(response)

            data = response.json()
            spaces = data.get("items", [])
            logger.info(f"{CYAN}Found {len(spaces)} space(s).{RESET}")
            return spaces

        except requests.RequestException as e:
            logger.error(f"{RED}Network error when fetching spaces: {str(e)}{RESET}")
            raise
        except Exception as e:
            logger.error(f"{RED}Unexpected error when fetching spaces: {str(e)}{RESET}")
            raise

    def get_space_by_id(self, space_id: str) -> Dict:
        """
        Get a specific space by its ID.
        GET /api/v1/spaces/{id}/
        """
        try:
            logger.info(f"{CYAN}Fetching space by ID: {space_id}{RESET}")
            response = self.session.get(f"{self.config.base_url}/spaces/{space_id}/", timeout=10)
            if not response.ok:
                self._handle_api_error(response)
            return response.json()
        except requests.RequestException as e:
            logger.error(f"{RED}Network error fetching space {space_id}: {str(e)}{RESET}")
            raise

    def find_space_by_name(self, space_name: str) -> Optional[Dict]:
        """
        Find a space (in the list of spaces) whose 'name' matches space_name.
        Returns the full space object if found, else None.
        """
        all_spaces = self.get_spaces()
        space_name_lower = space_name.strip().lower()
        for sp in all_spaces:
            if sp.get("name", "").strip().lower() == space_name_lower:
                return sp
        return None

    def get_websites_for_space(self) -> List[Dict]:
        """
        1) Determine which space to use (via config.space_id or config.space_name).
        2) GET that space's details, which include 'knowledge' → 'websites' → 'items'.
        3) Filter websites if self.config.website_filter is provided.
        """
        # 1. Determine space
        if not self.config.space_id and not self.config.space_name:
            logger.error(f"{RED}You must set either SPACE_ID or SPACE_NAME in the environment!{RESET}")
            sys.exit(1)

        if self.config.space_id:
            # Directly get the space by ID
            space_data = self.get_space_by_id(self.config.space_id)
        else:
            # Find space by name
            found = self.find_space_by_name(self.config.space_name)
            if not found:
                logger.error(f"{RED}Could not find a space named '{self.config.space_name}'. Exiting...{RESET}")
                sys.exit(1)
            self.config.space_id = found["id"]  # Store for future reference
            space_data = self.get_space_by_id(self.config.space_id)

        # 2. Extract websites from 'knowledge' → 'websites' → 'items'
        knowledge_data = space_data.get("knowledge", {})
        websites_data = knowledge_data.get("websites", {})
        all_websites = websites_data.get("items", [])
        logger.info(f"{CYAN}Space '{space_data.get('name')}' has {len(all_websites)} website(s).{RESET}")

        # 3. Filter if needed
        if not self.config.website_filter:
            logger.info(f"{CYAN}No WEBSITE_FILTER specified, returning all websites.{RESET}")
            return all_websites

        filtered = []
        for site in all_websites:
            site_id = str(site.get("id", "")).strip()
            site_name = str(site.get("name", "")).strip()
            site_url = str(site.get("url", "")).strip()

            # Because 'name' can be None, we handle it as empty string in filters
            site_identifiers = {
                self._normalize_url(site_id),
                self._normalize_url(site_name),
                self._normalize_url(site_url),
            }

            for filter_str in self.config.website_filter:
                clean_filter = self._normalize_url(filter_str)
                if clean_filter in site_identifiers:
                    filtered.append(site)
                    logger.info(f"{GREEN}Matched filter '{filter_str}' to site '{site_name or site_id}'{RESET}")
                    break

        logger.info(f"{CYAN}Filter matched {len(filtered)} of {len(all_websites)} websites in space.{RESET}")
        return filtered

    def trigger_crawl(self, website_id: str) -> Optional[Dict]:
        """Initiate a crawl with error handling"""
        try:
            logger.info(f"{CYAN}Triggering crawl for website {website_id}{RESET}")
            response = self.session.post(
                f"{self.config.base_url}/websites/{website_id}/run/",
                data="",
                timeout=30
            )

            if not response.ok:
                self._handle_api_error(response)

            return response.json()
        except requests.RequestException as e:
            logger.error(f"{RED}Crawl trigger failed: {str(e)}{RESET}")
            return None

    def get_crawl_status(self, website_id: str, run_id: str) -> Optional[str]:
        """Check crawl status with error handling"""
        try:
            logger.debug(f"Checking status for run_id={run_id}")
            response = self.session.get(
                f"{self.config.base_url}/websites/{website_id}/runs/",
                timeout=10
            )

            if not response.ok:
                self._handle_api_error(response)

            runs = response.json().get("items", [])
            return next(
                (run["status"] for run in runs if run["id"] == run_id),
                None
            )
        except requests.RequestException as e:
            logger.error(f"{RED}Status check failed: {str(e)}{RESET}")
            return None

def run_crawl_for_site(config: AppConfig, api_client: CrawlerAPIClient, site: Dict):
    """
    Trigger and monitor a crawl for a single website.
    This function is called by APScheduler for each site individually.
    """
    site_id = site.get("id")
    site_name = site.get("name") or site_id

    if not site_id:
        logger.error(f"{RED}Website missing ID, skipping...{RESET}")
        return
    
    logger.info(f"{MAGENTA}Starting crawl job for: {site_name}{RESET}")

    # Trigger crawl
    try:
        response = api_client.trigger_crawl(site_id)
        if not response:
            logger.error(f"{RED}Failed to start crawl for {site_name}{RESET}")
            return
        run_id = response["id"]
        logger.info(f"{GREEN}Started crawl for {site_name} (Run ID: {run_id}){RESET}")
    except Exception as e:
        logger.error(f"{RED}Error triggering crawl for {site_name}: {str(e)}{RESET}")
        return

    # Monitor until complete/failed
    logger.info(f"{MAGENTA}Crawl in progress for {site_name}; monitoring status...{RESET}")
    active = True
    while active:
        try:
            status = api_client.get_crawl_status(site_id, run_id)
            if not status:
                # Could not fetch or parse the status, keep trying
                logger.info(f"{YELLOW}Status not available yet for {site_name}{RESET}")
            else:
                if status == "complete":
                    logger.info(f"{GREEN}Completed crawl for {site_name} (Run ID: {run_id}){RESET}")
                    active = False
                elif status in ("failed", "cancelled"):
                    logger.error(f"{RED}Crawl {status} for {site_name} (Run ID: {run_id}){RESET}")
                    active = False
                else:
                    logger.info(f"{CYAN}{site_name} status: {status}{RESET}")

            if active:
                logger.info(f"{MAGENTA}Waiting {config.status_check_interval}s before next status check for {site_name}...{RESET}")
                time.sleep(config.status_check_interval)

        except Exception as e:
            logger.error(f"{RED}Status check error for {site_name}: {str(e)}{RESET}")
            logger.info(f"{MAGENTA}Waiting {config.status_check_interval}s before retry...{RESET}")
            time.sleep(config.status_check_interval)

    logger.info(f"{GREEN}Crawl job finished for {site_name}!{RESET}")

def run_all_sites_once(config: AppConfig, api_client: CrawlerAPIClient):
    """
    Test mode: runs all filtered websites a single time in a batch,
    similar to the old aggregator approach.
    """
    logger.info(f"{YELLOW}=== TEST MODE (single aggregated run) ==={RESET}")
    try:
        websites = api_client.get_websites_for_space()
    except Exception as e:
        logger.error(f"{RED}Failed to fetch websites in test mode: {str(e)}{RESET}")
        return

    if not websites:
        logger.warning(f"{YELLOW}No websites matched filter criteria in the space.{RESET}")
        return

    logger.info(f"{CYAN}Found {len(websites)} websites for test mode: "
                f"{[w.get('name') or w.get('id') for w in websites]}{RESET}")

    # Trigger crawls for each site (serially)
    for site in websites:
        run_crawl_for_site(config, api_client, site)

    logger.info(f"{YELLOW}=== TEST COMPLETE ==={RESET}")

def load_config() -> AppConfig:
    """Load and validate configuration with strict checks"""
    load_dotenv()
    
    parser = ArgumentParser(description="Website Crawler Scheduler")
    parser.add_argument("--test", action="store_true", help="Run once in test mode")
    parser.add_argument("--websites", type=str, help="Comma-separated website identifiers")
    args = parser.parse_args()

    api_key = os.getenv("API_KEY", "").strip()
    if not api_key.startswith("inp_"):
        logger.error(f"{RED}Invalid API key format - must start with 'inp_'{RESET}")
        sys.exit(1)

    base_url = os.getenv("BASE_URL", "https://sundsvall.backend.intric.ai/api/v1").strip()
    if not base_url.startswith(("http://", "https://")):
        logger.error(f"{RED}Invalid BASE_URL: {base_url}{RESET}")
        sys.exit(1)

    try:
        schedule = int(os.getenv("SCHEDULE_MINUTES", "300"))
        if schedule < 1:
            raise ValueError
    except ValueError:
        logger.error(f"{RED}SCHEDULE_MINUTES must be a positive integer{RESET}")
        sys.exit(1)

    # Collect filters from .env or --websites
    website_filter = set()
    if os.getenv("WEBSITE_FILTER"):
        website_filter.update(os.getenv("WEBSITE_FILTER").split(","))
    if args.websites:
        website_filter.update(args.websites.split(","))

    space_id = os.getenv("SPACE_ID", "").strip()
    space_name = os.getenv("SPACE_NAME", "").strip()

    return AppConfig(
        api_key=api_key,
        base_url=base_url,
        schedule_minutes=schedule,
        website_filter={s.strip().lower().rstrip('/') for s in website_filter if s.strip()},
        test_mode=args.test,
        space_id=space_id if space_id else None,
        space_name=space_name if space_name else None
    )

def main():
    """Main application workflow"""
    config = load_config()
    api_client = CrawlerAPIClient(config)
    
    # If user wants test mode, run all once in a batch
    if config.test_mode:
        run_all_sites_once(config, api_client)
        return

    # Otherwise, we schedule one job per site
    logger.info(f"{BLUE}Initializing scheduler with interval: {config.schedule_minutes} minutes{RESET}")
    scheduler = BackgroundScheduler(executors={'default': ThreadPoolExecutor(10)})

    # Fetch and filter the websites from the designated space
    try:
        websites = api_client.get_websites_for_space()
    except Exception as e:
        logger.error(f"{RED}Failed to fetch websites: {str(e)}{RESET}")
        sys.exit(1)

    if not websites:
        logger.warning(f"{YELLOW}No websites matched filter criteria. Exiting...{RESET}")
        return

    logger.info(f"{CYAN}Scheduling individual jobs for {len(websites)} website(s){RESET}")
    for site in websites:
        site_id = site.get("id")
        site_name = site.get("name", site_id)

        # Each site has its own interval-based job
        # max_instances=1 ensures we don't start a second crawl for the same site if the first hasn't finished.
        # coalesce=True means if multiple intervals are missed, we run only once after the site is free.
        job_id = f"crawl_{site_id}"  # unique job ID

        scheduler.add_job(
            run_crawl_for_site,
            'interval',
            minutes=config.schedule_minutes,
            args=[config, api_client, site],
            id=job_id,
            next_run_time=datetime.now() + timedelta(seconds=5),
            max_instances=1,
            coalesce=True
        )
        logger.info(f"{GREEN}Scheduled site '{site_name}' with interval={config.schedule_minutes} minutes (job_id={job_id}){RESET}")

    def shutdown(signum, frame):
        logger.info(f"{RED}Shutting down scheduler...{RESET}")
        scheduler.shutdown()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    scheduler.start()
    logger.info(f"{BLUE}Scheduler started. Press Ctrl+C to exit.{RESET}")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        shutdown(None, None)

if __name__ == "__main__":
    main()
