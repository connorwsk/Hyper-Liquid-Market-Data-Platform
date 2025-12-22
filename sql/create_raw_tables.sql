create schema if not exists raw;

create table if not exists raw.hyperliquid_meta_and_asset_ctxs_raw (
  id bigserial primary key,
  pulled_at timestamptz not null default now(),
  endpoint text not null default 'https://api.hyperliquid.xyz/info',
  request_type text not null,
  payload jsonb not null
);

create index if not exists idx_hl_raw_pulled_at
  on raw.hyperliquid_meta_and_asset_ctxs_raw (pulled_at);