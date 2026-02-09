CREATE TABLE IF NOT EXISTS products (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    category VARCHAR(100),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

INSERT INTO products (name, price, category, created_at, updated_at) VALUES
    ('Laptop', 999.99, 'electronics', '2024-01-01 10:00:00', '2024-01-01 10:00:00'),
    ('Mouse', 29.99, 'electronics', '2024-01-02 11:00:00', '2024-01-02 11:00:00'),
    ('Desk', 249.00, 'furniture', '2024-01-03 12:00:00', '2024-01-03 12:00:00'),
    ('Chair', 399.00, 'furniture', '2024-01-04 13:00:00', '2024-01-04 13:00:00'),
    ('Monitor', 549.99, 'electronics', '2024-01-05 14:00:00', '2024-01-05 14:00:00');
