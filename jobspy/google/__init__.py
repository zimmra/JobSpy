from __future__ import annotations

import math
import re
import json
from typing import Tuple
from datetime import datetime, timedelta
from urllib.parse import urlencode

from jobspy.google.constant import headers_jobs, headers_initial, async_param
from jobspy.model import (
    Scraper,
    ScraperInput,
    Site,
    JobPost,
    JobResponse,
    Location,
    JobType,
)
from jobspy.util import extract_emails_from_text, extract_job_type, create_session, flaresolverr_get
from jobspy.google.util import log, find_job_info_initial_page, find_job_info


class Google(Scraper):
    MIN_RESPONSE_LENGTH = 1000

    def __init__(
        self, proxies: list[str] | str | None = None, ca_cert: str | None = None, user_agent: str | None = None
    ):
        """
        Initializes Google Scraper with the Goodle jobs search url
        """
        site = Site(Site.GOOGLE)
        super().__init__(site, proxies=proxies, ca_cert=ca_cert)

        self.country = None
        self.session = None
        self.scraper_input = None
        self.jobs_per_page = 10
        self.seen_urls = set()
        self.url = "https://www.google.com/search"
        self.jobs_url = "https://www.google.com/async/callback:550"

    def scrape(self, scraper_input: ScraperInput) -> JobResponse:
        """
        Scrapes Google for jobs with scraper_input criteria.
        :param scraper_input: Information about job search criteria.
        :return: JobResponse containing a list of jobs.
        """
        self.scraper_input = scraper_input
        self.scraper_input.results_wanted = min(900, scraper_input.results_wanted)

        self.session = create_session(
            proxies=self.proxies, ca_cert=self.ca_cert, is_tls=False, has_retry=True
        )
        forward_cursor, job_list = self._get_initial_cursor_and_jobs()
        if forward_cursor is None:
            log.warning(
                "initial cursor not found, try changing your query or there was at most 10 results"
            )
            return JobResponse(jobs=job_list)

        page = 1

        while (
            len(self.seen_urls) < scraper_input.results_wanted + scraper_input.offset
            and forward_cursor
        ):
            log.info(
                f"search page: {page} / {math.ceil(scraper_input.results_wanted / self.jobs_per_page)}"
            )
            try:
                jobs, forward_cursor = self._get_jobs_next_page(forward_cursor)
            except Exception as e:
                log.error(f"failed to get jobs on page: {page}, {e}")
                break
            if not jobs:
                log.info(f"found no jobs on page: {page}")
                break
            job_list += jobs
            page += 1
        return JobResponse(
            jobs=job_list[
                scraper_input.offset : scraper_input.offset
                + scraper_input.results_wanted
            ]
        )

    def _get_initial_cursor_and_jobs(self) -> Tuple[str | None, list[JobPost]]:
        """Gets initial cursor and jobs to paginate through job listings"""
        query = f"{self.scraper_input.search_term} jobs"

        def get_time_range(hours_old):
            if hours_old <= 24:
                return "since yesterday"
            elif hours_old <= 72:
                return "in the last 3 days"
            elif hours_old <= 168:
                return "in the last week"
            else:
                return "in the last month"

        job_type_mapping = {
            JobType.FULL_TIME: "Full time",
            JobType.PART_TIME: "Part time",
            JobType.INTERNSHIP: "Internship",
            JobType.CONTRACT: "Contract",
        }

        if self.scraper_input.job_type in job_type_mapping:
            query += f" {job_type_mapping[self.scraper_input.job_type]}"

        if self.scraper_input.location:
            query += f" near {self.scraper_input.location}"

        if self.scraper_input.hours_old:
            time_filter = get_time_range(self.scraper_input.hours_old)
            query += f" {time_filter}"

        if self.scraper_input.is_remote:
            query += " remote"

        if self.scraper_input.google_search_term:
            query = self.scraper_input.google_search_term

        log.info(f"google search query: {query}")

        params = {"q": query, "udm": "8"}

        response_text = None

        # try direct request first
        log.debug(f"requesting {self.url} with params {params}")
        response = self.session.get(
            self.url, headers=headers_initial, params=params
        )
        log.debug(
            f"direct response: status={response.status_code}, "
            f"length={len(response.text)}"
        )

        if (
            response.status_code == 200
            and len(response.text) >= self.MIN_RESPONSE_LENGTH
            and "520084652" in response.text
        ):
            response_text = response.text
        else:
            # direct request failed, returned too little data, or is a JS
            # challenge page (Google returns 200 + large HTML with no job data)
            log.warning(
                f"direct request returned status {response.status_code} / "
                f"{len(response.text)} bytes (no job data marker)"
            )
            full_url = f"{self.url}?{urlencode(params)}"
            fs_result = flaresolverr_get(full_url)
            if fs_result is not None:
                log.info("using FlareSolverr fallback for Google jobs request")
                response_text = fs_result["response"]
            else:
                log.warning(
                    "FlareSolverr not configured or request failed; "
                    "cannot bypass Google bot detection"
                )

        if not response_text or len(response_text) < self.MIN_RESPONSE_LENGTH:
            log.error("Google returned an empty or very short response (possible CAPTCHA/block)")
            return None, []

        pattern_fc = r'<div jsname="Yust4d"[^>]+data-async-fc="([^"]+)"'
        match_fc = re.search(pattern_fc, response_text)
        data_async_fc = match_fc.group(1) if match_fc else None
        jobs_raw = find_job_info_initial_page(response_text)
        jobs = []
        for job_raw in jobs_raw:
            job_post = self._parse_job(job_raw)
            if job_post:
                jobs.append(job_post)
        log.info(f"initial page: {len(jobs)} jobs found, cursor={'yes' if data_async_fc else 'no'}")
        return data_async_fc, jobs

    def _get_jobs_next_page(self, forward_cursor: str) -> Tuple[list[JobPost], str]:
        params = {"fc": [forward_cursor], "fcv": ["3"], "async": [async_param]}
        response = self.session.get(self.jobs_url, headers=headers_jobs, params=params)
        return self._parse_jobs(response.text)

    def _parse_jobs(self, job_data: str) -> Tuple[list[JobPost], str]:
        """
        Parses jobs from a Google async callback response.

        Google's async callback format embeds job cards as rendered HTML in the
        response body.  Each card (jscontroller="b11o3b") contains title,
        company, location, date, job-type and description.  The next-page
        cursor lives in the ``jsname="Yust4d"`` div, same as the initial page.
        """
        from bs4 import BeautifulSoup

        # Next-page cursor – use the same Yust4d pattern as the initial page
        pattern_fc = r'<div jsname="Yust4d"[^>]+data-async-fc="([^"]+)"'
        match_fc = re.search(pattern_fc, job_data)
        data_async_fc = match_fc.group(1) if match_fc else None

        # The HTML section of the response contains rendered job cards
        html_start = job_data.find('<div jsname="iTtkOe">')
        if html_start == -1:
            log.debug("iTtkOe container not found in async response; no jobs parsed")
            return [], data_async_fc

        # HTML section ends where the [[[…]]] JSON array begins
        json_start = job_data.find("[[[", html_start)
        html_part = job_data[html_start:json_start] if json_start != -1 else job_data[html_start:]

        soup = BeautifulSoup(html_part, "html.parser")
        job_cards = soup.find_all(attrs={"jscontroller": "b11o3b"})

        jobs_on_page = []
        for card in job_cards:
            job_post = self._parse_job_card_html(card)
            if job_post:
                jobs_on_page.append(job_post)

        return jobs_on_page, data_async_fc

    def _parse_job_card_html(self, card) -> "JobPost | None":
        """Parses a single Google job card from the async callback HTML."""
        # Job ID and URL are on the inner qodLAe div
        inner = card.find(attrs={"jscontroller": "qodLAe"})
        if not inner:
            return None
        job_id = inner.get("id", "")
        if not job_id:
            return None

        # Canonical Google Jobs URL (used for deduplication and linking)
        share_url = card.get("data-share-url", "")
        # Extract the job-page URL fragment for a clean link
        htidocid_match = re.search(r"htidocid=([^&]+)", share_url)
        if htidocid_match:
            job_url = (
                "https://www.google.com/search?ibp=htl;jobs"
                f"&q&htidocid={htidocid_match.group(1)}"
            )
        else:
            job_url = share_url

        if job_url in self.seen_urls:
            return None
        self.seen_urls.add(job_url)

        title_elem = card.find(class_="tNxQIb")
        title = title_elem.get_text(strip=True) if title_elem else ""

        company_elem = card.find(class_="a3jPc")
        company_name = company_elem.get_text(strip=True) if company_elem else None

        # Location: "Fresno, CA  (+1 other)  •  via Talent.com"
        loc_elem = card.find(class_="FqK3wc")
        loc_raw = loc_elem.get_text(strip=True) if loc_elem else ""
        loc_clean = re.split(r"\s*\(|\s*•", loc_raw)[0].strip()
        city = state = country = None
        if loc_clean and "," in loc_clean:
            parts = [p.strip() for p in loc_clean.split(",")]
            city = parts[0]
            if len(parts) > 1:
                state = parts[1]

        # Date: "16 days ago" → date_posted
        date_posted = None
        date_elem = card.find(class_="K3eUK")
        if date_elem:
            days_ago_str = date_elem.get_text(strip=True)
            m = re.search(r"(\d+)\s+day", days_ago_str)
            if m:
                date_posted = (datetime.now() - timedelta(days=int(m.group(1)))).date()
            elif "yesterday" in days_ago_str.lower():
                date_posted = (datetime.now() - timedelta(days=1)).date()
            elif "today" in days_ago_str.lower() or "hour" in days_ago_str.lower():
                date_posted = datetime.now().date()

        # Description (serialised HTML embedded as text inside the card)
        description = ""
        desc_marker = card.find(string=re.compile(r"Job description"))
        if desc_marker:
            desc_container = desc_marker.find_parent()
            if desc_container:
                # The next sibling contains the description text
                sibling = desc_container.find_next_sibling()
                if sibling:
                    description = sibling.get_text(separator=" ", strip=True)

        return JobPost(
            id=f"go-{job_id}",
            title=title,
            company_name=company_name,
            location=Location(city=city, state=state, country=country),
            job_url=job_url,
            date_posted=date_posted,
            is_remote="remote" in description.lower() or "wfh" in description.lower(),
            description=description,
            emails=extract_emails_from_text(description),
            job_type=extract_job_type(description),
        )

    def _parse_job(self, job_info: list):
        job_url = job_info[3][0][0] if job_info[3] and job_info[3][0] else None
        if job_url in self.seen_urls:
            return
        self.seen_urls.add(job_url)

        title = job_info[0]
        company_name = job_info[1]
        location = city = job_info[2]
        state = country = date_posted = None
        if location and "," in location:
            city, state, *country = [*map(lambda x: x.strip(), location.split(","))]

        days_ago_str = job_info[12]
        if type(days_ago_str) == str:
            match = re.search(r"\d+", days_ago_str)
            days_ago = int(match.group()) if match else None
            date_posted = (datetime.now() - timedelta(days=days_ago)).date()

        description = job_info[19]

        job_post = JobPost(
            id=f"go-{job_info[28]}",
            title=title,
            company_name=company_name,
            location=Location(
                city=city, state=state, country=country[0] if country else None
            ),
            job_url=job_url,
            date_posted=date_posted,
            is_remote="remote" in description.lower() or "wfh" in description.lower(),
            description=description,
            emails=extract_emails_from_text(description),
            job_type=extract_job_type(description),
        )
        return job_post
