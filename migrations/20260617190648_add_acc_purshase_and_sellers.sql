CREATE TABLE sellers
(
    id SERIAL PRIMARY KEY,
    market_user_id BIGINT UNIQUE NOT NULL,
    username VARCHAR,
    can_buy BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE account_purchases
(
    account_id INT PRIMARY KEY REFERENCES accounts(id) ON DELETE CASCADE,
    seller_id INT NOT NULL REFERENCES sellers(id),
    price INT NOT NULL,
    purchased_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
)