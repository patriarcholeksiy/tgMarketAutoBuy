ALTER TABLE telegram_apps DROP CONSTRAINT IF EXISTS telegram_apps_api_id_key;
ALTER TABLE telegram_apps DROP CONSTRAINT IF EXISTS telegram_apps_api_hash_key;

ALTER TABLE telegram_apps ADD CONSTRAINT uq_telegram_apps_api UNIQUE (api_id, api_hash);

ALTER TABLE accounts ADD COLUMN tg_id BIGINT UNIQUE;