CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(200),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

INSERT INTO users (name, email, created_at, updated_at) VALUES
    ('Alice', 'alice@example.com', '2024-01-01 10:00:00', '2024-01-01 10:00:00'),
    ('Bob', 'bob@example.com', '2024-01-02 11:00:00', '2024-01-02 11:00:00'),
    ('Charlie', 'charlie@example.com', '2024-01-03 12:00:00', '2024-01-03 12:00:00'),
    ('Diana', 'diana@example.com', '2024-01-04 13:00:00', '2024-01-04 13:00:00'),
    ('Eve', 'eve@example.com', '2024-01-05 14:00:00', '2024-01-05 14:00:00');

CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    user_id INT NOT NULL,
    amount DECIMAL(10,2) NOT NULL,
    status VARCHAR(50) DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

INSERT INTO orders (user_id, amount, status, created_at, updated_at) VALUES
    (1, 99.99, 'completed', '2024-01-10 10:00:00', '2024-01-10 10:00:00'),
    (2, 149.50, 'completed', '2024-01-11 11:00:00', '2024-01-11 11:00:00'),
    (1, 29.99, 'pending', '2024-01-12 12:00:00', '2024-01-12 12:00:00'),
    (3, 199.00, 'completed', '2024-01-13 13:00:00', '2024-01-13 13:00:00'),
    (4, 75.00, 'cancelled', '2024-01-14 14:00:00', '2024-01-14 14:00:00');
