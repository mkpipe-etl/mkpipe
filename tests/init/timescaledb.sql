CREATE TABLE IF NOT EXISTS sensor_data (
    time TIMESTAMPTZ NOT NULL,
    sensor_id INT NOT NULL,
    temperature DOUBLE PRECISION,
    humidity DOUBLE PRECISION
);

SELECT create_hypertable('sensor_data', 'time', if_not_exists => TRUE);

INSERT INTO sensor_data (time, sensor_id, temperature, humidity) VALUES
    ('2024-01-01 10:00:00+00', 1, 22.5, 45.0),
    ('2024-01-01 11:00:00+00', 1, 23.0, 44.5),
    ('2024-01-01 12:00:00+00', 2, 21.0, 50.0),
    ('2024-01-02 10:00:00+00', 1, 22.0, 46.0),
    ('2024-01-02 11:00:00+00', 2, 20.5, 51.0);
