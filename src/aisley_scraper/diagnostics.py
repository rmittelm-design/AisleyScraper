from __future__ import annotations

from aisley_scraper.config import get_settings
from aisley_scraper.db.repository import Repository


def diagnose_staged_runs() -> None:
    settings = get_settings()
    repo = Repository(settings)

    sql = """
    select run_id,
           count(*) as staged_stores,
           max(scraped_at)::text as latest_scraped_at,
           (array_agg(website order by scraped_at desc))[1] as sample_website
    from shopify_stores_staging
    group by run_id
    order by staged_stores desc
    limit 20;
    """
    with repo._connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()

    if not rows:
        print("staged_runs=NONE")
        return

    print("staged_runs=")
    for run_id, staged_stores, latest_scraped_at, sample_website in rows:
        print(
            f"  run_id={run_id} staged_stores={staged_stores} "
            f"latest_scraped_at={latest_scraped_at or ''} sample_website={sample_website or ''}"
        )
