"""MCP server for JobSpy – exposes scrape_jobs as an MCP tool."""

from __future__ import annotations

from typing import Optional

from fastmcp import FastMCP

from jobspy import scrape_jobs

mcp = FastMCP(
    name="JobSpy",
    instructions=(
        "JobSpy lets you search for jobs across LinkedIn, Indeed, Glassdoor, "
        "ZipRecruiter, Google Jobs, Bayt, Naukri, and BDJobs in a single call. "
        "Use the search_jobs tool to search for jobs."
    ),
)


@mcp.tool
def search_jobs(
    search_term: Optional[str] = None,
    location: Optional[str] = None,
    site_name: Optional[list[str]] = None,
    results_wanted: int = 15,
    hours_old: Optional[int] = None,
    job_type: Optional[str] = None,
    is_remote: bool = False,
    country_indeed: str = "usa",
    google_search_term: Optional[str] = None,
    distance: int = 50,
    easy_apply: Optional[bool] = None,
    description_format: str = "markdown",
    linkedin_fetch_description: bool = False,
    linkedin_company_ids: Optional[list[int]] = None,
    offset: int = 0,
    enforce_annual_salary: bool = False,
) -> list[dict]:
    """Search for jobs across multiple job boards simultaneously.

    Args:
        search_term: The job title or keywords to search for (e.g. "software engineer").
        location: City, state, or country for the job search (e.g. "San Francisco, CA").
        site_name: List of job boards to search. Options: "linkedin", "indeed",
            "zip_recruiter", "glassdoor", "google", "bayt", "naukri", "bdjobs".
            Defaults to all sites.
        results_wanted: Number of results to retrieve per site. Default is 15.
        hours_old: Only return jobs posted within this many hours.
        job_type: Filter by employment type. One of: "fulltime", "parttime",
            "internship", "contract".
        is_remote: Filter for remote jobs only.
        country_indeed: Country to use for Indeed and Glassdoor searches (e.g. "usa",
            "canada", "uk"). Default is "usa".
        google_search_term: Custom search query for Google Jobs only (overrides the
            auto-constructed query). Example: "software engineer jobs near NYC since yesterday".
        distance: Search radius in miles. Default is 50.
        easy_apply: Filter for jobs that have easy apply on the job board.
        description_format: Format for job descriptions. Either "markdown" or "html".
            Default is "markdown".
        linkedin_fetch_description: Fetch full description and direct URL from LinkedIn
            (slower – makes one extra request per job). Default is False.
        linkedin_company_ids: Filter LinkedIn results to specific company IDs.
        offset: Start results from this offset (e.g. 25 skips the first 25 results).
        enforce_annual_salary: Convert all salaries to annual equivalents.

    Returns:
        A list of job postings, each represented as a dictionary with fields such as
        site, title, company, location, job_type, interval, min_amount, max_amount,
        job_url, and description.
    """
    df = scrape_jobs(
        site_name=site_name,
        search_term=search_term,
        google_search_term=google_search_term,
        location=location,
        distance=distance,
        is_remote=is_remote,
        job_type=job_type,
        easy_apply=easy_apply,
        results_wanted=results_wanted,
        country_indeed=country_indeed,
        description_format=description_format,
        linkedin_fetch_description=linkedin_fetch_description,
        linkedin_company_ids=linkedin_company_ids,
        offset=offset,
        hours_old=hours_old,
        enforce_annual_salary=enforce_annual_salary,
    )
    if df.empty:
        return []
    return df.where(df.notna(), other=None).to_dict(orient="records")


if __name__ == "__main__":
    mcp.run()
