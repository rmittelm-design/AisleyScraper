alter table if exists shopify_products
add column if not exists gender_probs_csv text;
