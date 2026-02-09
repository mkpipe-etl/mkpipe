db = db.getSiblingDB('source_db');

db.createCollection('customers');

db.customers.insertMany([
    { name: 'Alice', email: 'alice@example.com', age: 30, city: 'Istanbul', created_at: new Date('2024-01-01') },
    { name: 'Bob', email: 'bob@example.com', age: 25, city: 'Ankara', created_at: new Date('2024-01-02') },
    { name: 'Charlie', email: 'charlie@example.com', age: 35, city: 'Izmir', created_at: new Date('2024-01-03') },
    { name: 'Diana', email: 'diana@example.com', age: 28, city: 'Bursa', created_at: new Date('2024-01-04') },
    { name: 'Eve', email: 'eve@example.com', age: 32, city: 'Antalya', created_at: new Date('2024-01-05') },
]);
