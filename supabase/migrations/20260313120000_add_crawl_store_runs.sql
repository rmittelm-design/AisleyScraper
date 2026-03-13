create table if not exists public.crawl_store_runs (
  id bigserial primary key,
  run_id text not null,
  website text not null,
  status text not null check (status in ('pending', 'completed', 'failed')),
  attempt_count integer not null default 0,
  last_attempt_at timestamptz,
  error_message text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (run_id, website)
);

create index if not exists idx_crawl_store_runs_run_status
  on public.crawl_store_runs(run_id, status);

create index if not exists idx_crawl_store_runs_website
  on public.crawl_store_runs(website);
