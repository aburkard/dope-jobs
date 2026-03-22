"""Enrich company data from ATS board pages — logos, domains, descriptions."""
import json
import os
import re
import time
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
load_dotenv()

from db import get_connection, init_schema


def extract_greenhouse(token: str) -> dict:
    """Extract logo + domain from Greenhouse board page + API."""
    info = {}

    # API for name + description
    try:
        r = requests.get(f"https://boards-api.greenhouse.io/v1/boards/{token}", timeout=10)
        if r.ok:
            data = r.json()
            info["company_name"] = data.get("name")
            info["description"] = data.get("content", "")
    except Exception:
        pass

    # Board page for logo + domain
    try:
        r = requests.get(f"https://boards.greenhouse.io/{token}", timeout=10)
        if not r.ok:
            return info
        soup = BeautifulSoup(r.text, "lxml")

        # Logo: prioritized search for SQUARE images
        # 1. apple-touch-icon (always square, best quality)
        # 2. link rel=icon with large sizes (128px+)
        # 3. Greenhouse CDN /logos/ path in og:image
        # 4. img with "logo" in alt (only if square CDN URL pattern)
        logo_candidates = []

        # Check link tags for icons
        for link in soup.find_all("link", rel=True):
            rel = " ".join(link.get("rel", []))
            href = link.get("href", "")
            if not href or href.startswith("data:"):
                continue
            # Make absolute URL
            if href.startswith("//"):
                href = "https:" + href
            elif href.startswith("/"):
                href = f"https://boards.greenhouse.io{href}"
            if "apple-touch-icon" in rel:
                logo_candidates.insert(0, href)  # Best option
            elif "icon" in rel:
                # Prefer larger icons
                sizes = link.get("sizes", "")
                if sizes and any(int(s.split("x")[0]) >= 128 for s in sizes.split() if "x" in s):
                    logo_candidates.insert(min(1, len(logo_candidates)), href)

        # Check og:image — only use if from Greenhouse CDN /logos/ path (those are square)
        og = soup.find("meta", property="og:image")
        if og and og.get("content"):
            og_url = og["content"]
            if "/logos/" in og_url:
                logo_candidates.insert(min(1, len(logo_candidates)), og_url)

        if logo_candidates:
            info["logo_url"] = logo_candidates[0]

        # Description from og:description
        og_desc = soup.find("meta", property="og:description")
        if og_desc and og_desc.get("content"):
            desc = og_desc["content"].strip()
            # Strip HTML from description
            if "<" in desc:
                desc = BeautifulSoup(desc, "lxml").get_text()
            if desc and len(desc) > len(info.get("description", "")):
                info["description"] = desc

        # Social links
        socials = {}
        for a in soup.find_all("a", href=True):
            href = a["href"]
            for platform in ["twitter.com", "x.com", "linkedin.com", "github.com"]:
                if platform in href:
                    socials[platform.split(".")[0]] = href
        if socials:
            info["socials"] = socials

        # Domain: find external links, pick the most common domain
        domains = {}
        for link in soup.find_all("a", href=True):
            href = link["href"]
            if not href.startswith("http"):
                continue
            parsed = urlparse(href)
            host = parsed.hostname or ""
            # Skip Greenhouse, Google, and CDN domains
            if any(skip in host for skip in ["greenhouse", "google", "cdn", "googleapis"]):
                continue
            # Strip www
            if host.startswith("www."):
                host = host[4:]
            if host:
                domains[host] = domains.get(host, 0) + 1

        if domains:
            # Pick the most frequently linked domain
            best_domain = max(domains, key=domains.get)
            info["domain"] = best_domain

    except Exception as e:
        info["_error"] = str(e)

    return info


def extract_lever(token: str) -> dict:
    """Extract logo + domain from Lever board page."""
    info = {}
    try:
        r = requests.get(f"https://jobs.lever.co/{token}", timeout=10)
        if not r.ok:
            return info
        soup = BeautifulSoup(r.text, "lxml")

        # Logo: prioritize S3 client logos (square), then og:image
        logo_candidates = []
        for img in soup.find_all("img"):
            src = img.get("src", "")
            alt = img.get("alt", "").lower()
            if not src or src.startswith("data:") or src.startswith("/img/lever"):
                continue
            # Make absolute
            if src.startswith("/"):
                src = f"https://jobs.lever.co{src}"
            # Lever S3 client logos are square
            if "lever-client-logos" in src:
                logo_candidates.insert(0, src)
            elif "logo" in alt and "lever" not in alt:
                logo_candidates.append(src)
        # og:image — usually the client logo on Lever
        og = soup.find("meta", property="og:image")
        if og and og.get("content") and "lever-client-logos" in og["content"]:
            logo_candidates.insert(0, og["content"])
        if logo_candidates:
            info["logo_url"] = logo_candidates[0]

        # Domain: look in footer links or any external link
        domains = {}
        for link in soup.find_all("a", href=True):
            href = link["href"]
            if not href.startswith("http"):
                continue
            parsed = urlparse(href)
            host = parsed.hostname or ""
            if any(skip in host for skip in ["lever.co", "google", "cdn", "fonts"]):
                continue
            if host.startswith("www."):
                host = host[4:]
            if host:
                domains[host] = domains.get(host, 0) + 1
        if domains:
            info["domain"] = max(domains, key=domains.get)

        # Company name from page title
        title_tag = soup.find("title")
        if title_tag:
            # Usually "Company Name - Jobs"
            title = title_tag.text.strip()
            if " - " in title:
                info["company_name"] = title.split(" - ")[0].strip()
            elif " | " in title:
                info["company_name"] = title.split(" | ")[0].strip()

    except Exception as e:
        info["_error"] = str(e)
    return info


def extract_ashby(token: str) -> dict:
    """Extract logo + domain from Ashby GraphQL API + board page."""
    info = {}

    # GraphQL API
    try:
        r = requests.post("https://jobs.ashbyhq.com/api/non-user-graphql", json={
            "operationName": "ApiJobBoardWithTeams",
            "variables": {"organizationHostedJobsPageName": token},
            "query": """query ApiJobBoardWithTeams($organizationHostedJobsPageName: String!) {
                jobBoard: jobBoardWithTeams(organizationHostedJobsPageName: $organizationHostedJobsPageName) {
                    title
                    descriptionPlain
                    logoImageLink
                    publicWebsite
                    jobPostings { id }
                }
            }"""
        }, timeout=10)
        if r.ok:
            board = r.json().get("data", {}).get("jobBoard", {})
            if board:
                info["company_name"] = board.get("title")
                info["description"] = board.get("descriptionPlain", "")
                if board.get("logoImageLink"):
                    info["logo_url"] = board["logoImageLink"]
                website = board.get("publicWebsite", "")
                if website:
                    parsed = urlparse(website if "://" in website else f"https://{website}")
                    host = parsed.hostname or ""
                    if host.startswith("www."):
                        host = host[4:]
                    info["domain"] = host
                info["job_count"] = len(board.get("jobPostings", []))
    except Exception as e:
        info["_error"] = str(e)

    # Also scrape the board page for logo/domain if API didn't provide
    if not info.get("logo_url") or not info.get("domain"):
        try:
            r = requests.get(f"https://jobs.ashbyhq.com/{token}", timeout=10)
            if r.ok:
                soup = BeautifulSoup(r.text, "lxml")

                # og:image for logo
                if not info.get("logo_url"):
                    og = soup.find("meta", property="og:image")
                    if og and og.get("content"):
                        info["logo_url"] = og["content"]

                # og:title for company name
                if not info.get("company_name"):
                    og_title = soup.find("meta", property="og:title")
                    if og_title and og_title.get("content"):
                        title = og_title["content"]
                        # Usually "Company - Jobs" or similar
                        for sep in [" - ", " | ", " — "]:
                            if sep in title:
                                title = title.split(sep)[0].strip()
                                break
                        info["company_name"] = title

                # External links for domain
                if not info.get("domain"):
                    domains = {}
                    for a in soup.find_all("a", href=True):
                        href = a["href"]
                        if not href.startswith("http"):
                            continue
                        host = urlparse(href).hostname or ""
                        if any(skip in host for skip in ["ashbyhq", "google", "cdn"]):
                            continue
                        if host.startswith("www."):
                            host = host[4:]
                        if host:
                            domains[host] = domains.get(host, 0) + 1
                    if domains:
                        info["domain"] = max(domains, key=domains.get)
        except Exception:
            pass

    return info


EXTRACTORS = {
    "greenhouse": extract_greenhouse,
    "lever": extract_lever,
    "ashby": extract_ashby,
}


def main():
    companies = []
    with open("companies.txt") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if ":" in line:
                ats, token = line.split(":", 1)
                companies.append((ats.strip(), token.strip()))

    conn = get_connection()
    init_schema(conn)

    # Add logo_url column if not exists
    with conn.cursor() as cur:
        cur.execute("""
            ALTER TABLE pipeline_companies
            ADD COLUMN IF NOT EXISTS logo_url TEXT,
            ADD COLUMN IF NOT EXISTS description TEXT
        """)
    conn.commit()

    print(f"Enriching {len(companies)} companies from board pages...\n")

    for ats, token in companies:
        extractor = EXTRACTORS.get(ats)
        if not extractor:
            print(f"  {ats}:{token:25s} — no extractor")
            continue

        info = extractor(token)

        name = info.get("company_name", "")
        domain = info.get("domain", "")
        logo = info.get("logo_url", "")
        desc_len = len(info.get("description", ""))
        error = info.get("_error", "")

        logo_short = "yes" if logo else "—"
        print(f"  {ats}:{token:25s} name={name:25s} domain={domain:25s} logo={logo_short:4s} desc={desc_len}ch")
        if error:
            print(f"    ERROR: {error}")

        # Update DB
        with conn.cursor() as cur:
            updates = []
            vals = []
            if name:
                updates.append("company_name = %s")
                vals.append(name)
            if domain:
                updates.append("domain = %s")
                vals.append(domain)
            if logo:
                updates.append("logo_url = %s")
                vals.append(logo)
            if info.get("description"):
                updates.append("description = %s")
                vals.append(info["description"])

            if updates:
                vals.extend([ats, token])
                cur.execute(
                    f"UPDATE pipeline_companies SET {', '.join(updates)} WHERE ats = %s AND board_token = %s",
                    vals
                )
        conn.commit()
        time.sleep(0.3)

    # Show results
    print(f"\n=== Results ===")
    with conn.cursor() as cur:
        cur.execute("""
            SELECT ats, board_token, company_name, domain, logo_url IS NOT NULL as has_logo
            FROM pipeline_companies ORDER BY company_name
        """)
        has_logo = 0
        has_domain = 0
        for r in cur.fetchall():
            logo_icon = "L" if r[4] else " "
            domain_icon = "D" if r[3] else " "
            if r[4]: has_logo += 1
            if r[3]: has_domain += 1
            print(f"  [{logo_icon}{domain_icon}] {r[2] or r[1]:30s} {r[3] or '':25s}")
        total = cur.rowcount
    print(f"\n{has_logo}/{total} have logos, {has_domain}/{total} have domains")

    conn.close()


if __name__ == "__main__":
    main()
