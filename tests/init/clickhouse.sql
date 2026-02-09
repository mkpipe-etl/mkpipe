CREATE TABLE IF NOT EXISTS source_db.events (
    id UInt64,
    event_type String,
    user_id UInt64,
    payload String,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY id;

INSERT INTO source_db.events (id, event_type, user_id, payload, created_at) VALUES
    (1, 'page_view', 1, '{"page": "/home"}', '2024-01-01 10:00:00'),
    (2, 'click', 2, '{"button": "signup"}', '2024-01-02 11:00:00'),
    (3, 'page_view', 1, '{"page": "/products"}', '2024-01-03 12:00:00'),
    (4, 'purchase', 3, '{"product_id": 42}', '2024-01-04 13:00:00'),
    (5, 'click', 4, '{"button": "checkout"}', '2024-01-05 14:00:00');
