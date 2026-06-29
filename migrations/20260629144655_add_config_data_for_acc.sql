ALTER TABLE accounts
    ADD COLUMN device_model VARCHAR(255),
    ADD COLUMN system_version VARCHAR(127),
    ADD COLUMN app_version VARCHAR(127);