from __future__ import annotations

import json
import math
import re
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from urllib.parse import urlencode

from bs4 import BeautifulSoup

from jobspy.ziprecruiter.constant import headers, get_cookie_data
from jobspy.util import (
    extract_emails_from_text,
    create_session,
    markdown_converter,
    remove_attributes,
    create_logger,
    flaresolverr_get,
    FLARESOLVERR_URL,
)
from jobspy.model import (
    JobPost,
    Compensation,
    Location,
    JobResponse,
    Country,
    DescriptionFormat,
    Scraper,
    ScraperInput,
    Site,
    JobType,
)
from jobspy.ziprecruiter.util import get_job_type_enum, add_params

log = create_logger("ZipRecruiter")

# ZipRecruiter website job-card enum mappings
_PAY_INTERVAL_MAP = {1: "hourly", 5: "yearly"}
_EMP_TYPE_MAP = {1: JobType.FULL_TIME, 2: JobType.PART_TIME}
# locationTypes.name: 3 = remote
_REMOTE_LOCATION_TYPES = {3}


class ZipRecruiter(Scraper):
    base_url = "https://www.ziprecruiter.com"
    api_url = "https://api.ziprecruiter.com"

    def __init__(
        self, proxies: list[str] | str | None = None, ca_cert: str | None = None, user_agent: str | None = None
    ):
        """
        Initializes ZipRecruiterScraper with the ZipRecruiter job search url
        """
        super().__init__(Site.ZIP_RECRUITER, proxies=proxies)

        self.scraper_input = None
        self.session = create_session(
            proxies=proxies, ca_cert=ca_cert, client_identifier="safari_ios_16_0"
        )
        self.session.headers.update(headers)
        self._get_cookies()

        self.delay = 5
        self.jobs_per_page = 20
        self.seen_urls = set()

    def scrape(self, scraper_input: ScraperInput) -> JobResponse:
        """
        Scrapes ZipRecruiter for jobs with scraper_input criteria.

        When FlareSolverr is configured the website is scraped directly
        (bypassing the mobile API whose auth token is no longer valid).
        Otherwise the legacy mobile API is used as a best-effort fallback.

        :param scraper_input: Information about job search criteria.
        :return: JobResponse containing a list of jobs.
        """
        self.scraper_input = scraper_input

        if FLARESOLVERR_URL:
            log.info("FlareSolverr configured – scraping website directly")
            job_list = self._scrape_website(scraper_input)
        else:
            log.warning(
                "FlareSolverr not configured – falling back to legacy mobile API "
                "(results may be empty; set FLARESOLVERR_URL for reliable results)"
            )
            job_list = self._scrape_api(scraper_input)

        return JobResponse(jobs=job_list[: scraper_input.results_wanted])

    # ------------------------------------------------------------------
    # Website scraping (via FlareSolverr)
    # ------------------------------------------------------------------

    def _scrape_website(self, scraper_input: ScraperInput) -> list[JobPost]:
        """Scrapes the ZipRecruiter jobs-search website pages via FlareSolverr."""
        jobs: list[JobPost] = []
        days = max(scraper_input.hours_old // 24, 1) if scraper_input.hours_old else None
        max_pages = math.ceil(scraper_input.results_wanted / self.jobs_per_page)

        for page in range(1, max_pages + 1):
            if len(jobs) >= scraper_input.results_wanted:
                break

            log.info(f"search page: {page} / {max_pages}")

            params: dict = {"search": scraper_input.search_term}
            if scraper_input.location:
                params["location"] = scraper_input.location
            if scraper_input.distance:
                params["radius"] = scraper_input.distance
            if days:
                params["days"] = days
            if scraper_input.is_remote:
                params["remote"] = 1
            params["page"] = page

            url = f"{self.base_url}/jobs-search?{urlencode(params)}"
            log.debug(f"fetching via FlareSolverr: {url}")

            fs_result = flaresolverr_get(url)
            if fs_result is None:
                log.error("FlareSolverr request failed")
                break

            page_jobs = self._parse_website_page(fs_result["response"])
            log.debug(f"parsed {len(page_jobs)} jobs from page {page}")
            if not page_jobs:
                log.info(f"no jobs on page {page}, stopping")
                break

            jobs.extend(page_jobs)

            if page < max_pages:
                time.sleep(self.delay)

        return jobs

    def _parse_website_page(self, html: str) -> list[JobPost]:
        """Parses job cards from a ZipRecruiter search-results HTML page."""
        # The page embeds all job data as a JSON blob in a <script> tag
        scripts = re.findall(r"<script[^>]*>(.*?)</script>", html, re.DOTALL)
        page_data = None
        for s in scripts:
            if "hydrateJobCardsResponse" in s:
                try:
                    page_data = json.loads(s)
                    break
                except json.JSONDecodeError as exc:
                    log.debug(f"JSON decode error in page script: {exc}")

        if page_data is None:
            log.warning("hydrateJobCardsResponse not found in page")
            return []

        job_cards = page_data.get("hydrateJobCardsResponse", {}).get("jobCards", [])
        jobs = []
        for card in job_cards:
            job = self._parse_website_job_card(card)
            if job:
                jobs.append(job)
        return jobs

    def _parse_website_job_card(self, card: dict) -> JobPost | None:
        """Converts a single hydrateJobCardsResponse job card to a JobPost."""
        listing_key = card.get("listingKey", "")
        job_url = f"{self.base_url}/jobs//j?lvk={listing_key}"

        if job_url in self.seen_urls:
            return None
        self.seen_urls.add(job_url)

        title = card.get("title", "")
        company = card.get("company", {}).get("name")
        description = card.get("shortDescription", "")

        # Location
        loc = card.get("location", {})
        country_value = "usa" if loc.get("countryCode") == "US" else "canada"
        country_enum = Country.from_string(country_value)
        location = Location(
            city=loc.get("city"),
            state=loc.get("stateCode") or loc.get("state"),
            country=country_enum,
        )

        # Date posted
        posted_at = card.get("status", {}).get("postedAtUtc", "")
        date_posted = (
            datetime.fromisoformat(posted_at.rstrip("Z")).date() if posted_at else None
        )

        # Compensation
        pay = card.get("pay", {})
        comp = None
        if pay and pay.get("min") is not None:
            interval = _PAY_INTERVAL_MAP.get(pay.get("interval"))
            comp = Compensation(
                interval=interval,
                min_amount=pay.get("min"),
                max_amount=pay.get("max"),
                currency="USD",
            )

        # Job type
        job_types = [
            _EMP_TYPE_MAP[et["name"]]
            for et in card.get("employmentTypes", [])
            if et.get("name") in _EMP_TYPE_MAP
        ]

        # Remote
        is_remote = any(
            lt.get("name") in _REMOTE_LOCATION_TYPES
            for lt in card.get("locationTypes", [])
        )

        if self.scraper_input and self.scraper_input.description_format == DescriptionFormat.MARKDOWN:
            description = markdown_converter(description) if description else description

        return JobPost(
            id=f"zr-{listing_key}",
            title=title,
            company_name=company,
            location=location,
            job_type=job_types if job_types else None,
            compensation=comp,
            date_posted=date_posted,
            job_url=job_url,
            description=description,
            emails=extract_emails_from_text(description) if description else None,
            is_remote=is_remote,
        )

    # ------------------------------------------------------------------
    # Legacy mobile API (fallback when FlareSolverr is not configured)
    # ------------------------------------------------------------------

    def _scrape_api(self, scraper_input: ScraperInput) -> list[JobPost]:
        """Scrapes ZipRecruiter via the legacy mobile API."""
        job_list: list[JobPost] = []
        continue_token = None

        max_pages = math.ceil(scraper_input.results_wanted / self.jobs_per_page)
        for page in range(1, max_pages + 1):
            if len(job_list) >= scraper_input.results_wanted:
                break
            if page > 1:
                time.sleep(self.delay)
            log.info(f"search page: {page} / {max_pages}")
            jobs_on_page, continue_token = self._find_jobs_in_page(
                scraper_input, continue_token
            )
            if jobs_on_page:
                job_list.extend(jobs_on_page)
            else:
                break
            if not continue_token:
                break
        return job_list

    def _find_jobs_in_page(
        self, scraper_input: ScraperInput, continue_token: str | None = None
    ) -> tuple[list[JobPost], str | None]:
        """
        Scrapes a page of ZipRecruiter for jobs with scraper_input criteria
        :param scraper_input:
        :param continue_token:
        :return: jobs found on page
        """
        jobs_list = []
        params = add_params(scraper_input)
        if continue_token:
            params["continue_from"] = continue_token
        try:
            log.debug(
                f"requesting {self.api_url}/jobs-app/jobs with params {params}"
            )
            res = self.session.get(f"{self.api_url}/jobs-app/jobs", params=params)
            log.debug(
                f"api response: status={res.status_code}, length={len(res.text)}"
            )
            if res.status_code not in range(200, 400):
                if res.status_code == 429:
                    err = "429 Response - Blocked by ZipRecruiter for too many requests"
                else:
                    err = f"ZipRecruiter response status code {res.status_code}"
                    err += f" with response: {res.text}"  # ZipRecruiter likely not available in EU
                log.error(err)
                return jobs_list, ""
        except Exception as e:
            if "Proxy responded with" in str(e):
                log.error(f"ZipRecruiter: Bad proxy")
            else:
                log.error(f"ZipRecruiter: {str(e)}")
            return jobs_list, ""

        res_data = res.json()
        jobs_list = res_data.get("jobs", [])
        next_continue_token = res_data.get("continue", None)
        if not jobs_list:
            log.warning(f"ZipRecruiter returned 0 jobs (status {res.status_code})")
        with ThreadPoolExecutor(max_workers=self.jobs_per_page) as executor:
            job_results = [executor.submit(self._process_job, job) for job in jobs_list]

        job_list = list(filter(None, (result.result() for result in job_results)))
        return job_list, next_continue_token

    def _process_job(self, job: dict) -> JobPost | None:
        """
        Processes an individual job dict from the response
        """
        title = job.get("name")
        job_url = f"{self.base_url}/jobs//j?lvk={job['listing_key']}"
        if job_url in self.seen_urls:
            return
        self.seen_urls.add(job_url)

        description = job.get("job_description", "").strip()
        listing_type = job.get("buyer_type", "")
        description = (
            markdown_converter(description)
            if self.scraper_input.description_format == DescriptionFormat.MARKDOWN
            else description
        )
        company = job.get("hiring_company", {}).get("name")
        country_value = "usa" if job.get("job_country") == "US" else "canada"
        country_enum = Country.from_string(country_value)

        location = Location(
            city=job.get("job_city"), state=job.get("job_state"), country=country_enum
        )
        job_type = get_job_type_enum(
            job.get("employment_type", "").replace("_", "").lower()
        )
        date_posted = datetime.fromisoformat(job["posted_time"].rstrip("Z")).date()
        comp_interval = job.get("compensation_interval")
        comp_interval = "yearly" if comp_interval == "annual" else comp_interval
        comp_min = int(job["compensation_min"]) if "compensation_min" in job else None
        comp_max = int(job["compensation_max"]) if "compensation_max" in job else None
        comp_currency = job.get("compensation_currency")
        description_full, job_url_direct = self._get_descr(job_url)

        return JobPost(
            id=f'zr-{job["listing_key"]}',
            title=title,
            company_name=company,
            location=location,
            job_type=job_type,
            compensation=Compensation(
                interval=comp_interval,
                min_amount=comp_min,
                max_amount=comp_max,
                currency=comp_currency,
            ),
            date_posted=date_posted,
            job_url=job_url,
            description=description_full if description_full else description,
            emails=extract_emails_from_text(description) if description else None,
            job_url_direct=job_url_direct,
            listing_type=listing_type,
        )

    def _get_descr(self, job_url):
        res = self.session.get(job_url, allow_redirects=True)
        description_full = job_url_direct = None
        if res.ok:
            soup = BeautifulSoup(res.text, "html.parser")
            job_descr_div = soup.find("div", class_="job_description")
            company_descr_section = soup.find("section", class_="company_description")
            job_description_clean = (
                remove_attributes(job_descr_div).prettify(formatter="html")
                if job_descr_div
                else ""
            )
            company_description_clean = (
                remove_attributes(company_descr_section).prettify(formatter="html")
                if company_descr_section
                else ""
            )
            description_full = job_description_clean + company_description_clean

            try:
                script_tag = soup.find("script", type="application/json")
                if script_tag:
                    job_json = json.loads(script_tag.string)
                    job_url_val = job_json["model"].get("saveJobURL", "")
                    m = re.search(r"job_url=(.+)", job_url_val)
                    if m:
                        job_url_direct = m.group(1)
            except:
                job_url_direct = None

            if self.scraper_input.description_format == DescriptionFormat.MARKDOWN:
                description_full = markdown_converter(description_full)

        return description_full, job_url_direct

    def _get_cookies(self):
        """
        Sends a session event to the API with device properties.
        """
        url = f"{self.api_url}/jobs-app/event"
        log.debug(f"sending session event to {url}")
        self.session.post(url, data=get_cookie_data)
