CREATE TABLE telegram_apps
(
    id SERIAL PRIMARY KEY,
    api_id INT NOT NULL UNIQUE ,
    api_hash VARCHAR(32) NOT NULL UNIQUE
);

CREATE TYPE account_role AS ENUM ('viewer', 'buyer');

CREATE TYPE account_status AS ENUM ('active', 'frozen', 'unauthorized');

CREATE TABLE accounts
(
    id SERIAL PRIMARY KEY,
    app_id INT REFERENCES telegram_apps(id),
    dc_id INT,
    phone VARCHAR(20),
    session TEXT NOT NULL,
    role account_role DEFAULT 'viewer' NOT NULL,
    status account_status DEFAULT 'active' NOT NULL,
    status_updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- create function trigger for update status time when status changed
CREATE OR REPLACE FUNCTION update_status_time()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.status IS DISTINCT FROM OLD.status THEN
        NEW.status_updated_at = CURRENT_TIMESTAMP;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- add trigger for accounts before update
CREATE TRIGGER trigger_update_status_time
BEFORE UPDATE ON accounts
FOR EACH ROW EXECUTE FUNCTION update_status_time();

CREATE TABLE collections
(
    id SERIAL PRIMARY KEY,
    title VARCHAR(30) NOT NULL,
    short_title VARCHAR(25) NOT NULL,
    tg_id BIGINT NOT NULL UNIQUE,
    active BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE models
(
    id SERIAL PRIMARY KEY,
    name VARCHAR(30)                           NOT NULL,
    collection_id INT REFERENCES collections(id) NOT NULL,
    floor_price INT                            NOT NULL DEFAULT 125 CHECK ( floor_price >= 125 ),
    sticker_id BIGINT                          NOT NULL,
    crafted BOOLEAN DEFAULT FALSE              NOT NULL
    -- provides uniqueness in combinations_coeffients
    -- CONSTRAINT unique_model_gift UNIQUE (id, collection_id) -- add later to another migration
);

CREATE TABLE backdrops
(
    id SERIAL PRIMARY KEY,
    name VARCHAR(30)                               NOT NULL,
    collection_id INT REFERENCES collections(id)  NOT NULL,
    floor_price INT                                NOT NULL DEFAULT 125 CHECK ( floor_price >= 125 ),
    tg_backdrop_id INT                             NOT NULL
    -- provides uniqueness in combinations_coeffients
    -- CONSTRAINT unique_backdrop_gift UNIQUE (id, collection_id) -- add later to another migration
);

-- CREATE TABLE combinations_coeffients
-- (
--     id SERIAL PRIMARY KEY,
--     collection_id INT REFERENCES collections(id) NOT NULL,
--     sticker_id INT                                 NOT NULL,
--     tg_backdrop_id INT                             NOT NULL,
--
--     min_number INT,
--     max_number INT,
--
--     coefficient NUMERIC(6, 2)                      NOT NULL DEFAULT 1
--
-- )

