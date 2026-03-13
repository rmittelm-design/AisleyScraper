create extension if not exists pgcrypto;

do $$
declare
	item_uuid_udt text;
begin
	select c.udt_name
	into item_uuid_udt
	from information_schema.columns as c
	where c.table_schema = 'public'
		and c.table_name = 'shopify_products'
		and c.column_name = 'item_uuid';

	if item_uuid_udt is null then
		alter table public.shopify_products add column item_uuid text;
		item_uuid_udt := 'text';
	end if;

	if item_uuid_udt <> 'uuid' then
		update public.shopify_products
		set item_uuid = gen_random_uuid()::text
		where item_uuid is null
			or item_uuid !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$';

		alter table public.shopify_products
		alter column item_uuid type uuid
		using item_uuid::uuid;
	end if;

	alter table public.shopify_products
	alter column item_uuid set default gen_random_uuid();

	update public.shopify_products
	set item_uuid = gen_random_uuid()
	where item_uuid is null;

	alter table public.shopify_products
	alter column item_uuid set not null;
end $$;