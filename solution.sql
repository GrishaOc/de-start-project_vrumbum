-- Этап 1. Создание и заполнение БД
-- Шаг 2
CREATE SCHEMA raw_data;
CREATE TABLE raw_data.sales ();
-- Шаг 3
COPY raw_data.sales FROM '/path/to/cars.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',');
-- Шаг 5
CREATE SCHEMA IF NOT EXISTS car_shop;
CREATE TABLE car_shop.brands (
    brand_id SERIAL PRIMARY KEY,-- Автоинкрементный целочисленный ID
    brand_name VARCHAR(50) NOT NULL, -- Название бренда (ограничение 50 символов)
    origin_country VARCHAR(50) NOT NULL -- Страна производства
);
CREATE TABLE car_shop.colors (
    color_id SERIAL PRIMARY KEY,  -- Автоинкрементный ID
    color_name VARCHAR(30) NOT NULL UNIQUE -- Название цвета (уникальное)
);
CREATE TABLE car_shop.customers (
    customer_id SERIAL PRIMARY KEY, -- Автоинкрементный ID
    customer_name VARCHAR(100) NOT NULL, -- Имя покупателя
    phone VARCHAR(30) NOT NULL -- Номер телефона
);
CREATE TABLE car_shop.car_models (
    model_id SERIAL PRIMARY KEY,
    brand_id INTEGER NOT NULL REFERENCES car_shop.brands(brand_id), -- Внешний ключ на brands
    model_name VARCHAR(50) NOT NULL, -- Название модели (A3, Model X)
    base_gasoline_consumption NUMERIC(5,2) NULL, -- Расход топлива (л/100км)
    CONSTRAINT unique_model_per_brand UNIQUE (brand_id, model_name)
);
CREATE TABLE car_shop.cars (
    car_id INTEGER PRIMARY KEY, -- ID из исходных данных (не SERIAL)
    model_id INTEGER NOT NULL REFERENCES car_shop.car_models(model_id), -- Внешний ключ на car_models
    color_id INTEGER REFERENCES car_shop.colors(color_id),
    price NUMERIC(12,2) NOT NULL CHECK (price > 0), -- Цена (2 знака после запятой)
    sale_date DATE NOT NULL, -- Дата продажи
    discount NUMERIC(5,2) NOT NULL DEFAULT 0 CHECK (discount >= 0 AND discount <= 100), -- Скидка (0-100%)
    customer_id INTEGER NOT NULL REFERENCES car_shop.customers(customer_id) -- Внешний ключ на customers
);
-- Шаг 6,7
INSERT INTO car_shop.colors (color_name)
SELECT DISTINCT TRIM(color)
FROM (
    SELECT UNNEST(STRING_TO_ARRAY(SPLIT_PART(auto, ', ', 2), ', ')) AS color
    FROM raw_data.sales
) AS colors
ON CONFLICT (color_name) DO NOTHING;
ALTER TABLE car_shop.brands 
ADD CONSTRAINT brands_name_country_unique UNIQUE (brand_name, origin_country);
INSERT INTO car_shop.brands (brand_name, origin_country)
SELECT DISTINCT 
    CASE 
        WHEN auto LIKE 'Lada%' THEN 'Lada'
        WHEN auto LIKE 'BMW%' THEN 'BMW'
        WHEN auto LIKE 'Audi%' THEN 'Audi'
        WHEN auto LIKE 'Tesla%' THEN 'Tesla'
        WHEN auto LIKE 'Hyundai%' THEN 'Hyundai'
        WHEN auto LIKE 'Kia%' THEN 'Kia'
        WHEN auto LIKE 'Porsche%' THEN 'Porsche'
    END,
    COALESCE(brand_origin, 'Unknown')
FROM raw_data.sales
ON CONFLICT (brand_name, origin_country) DO NOTHING;
ALTER TABLE car_shop.customers 
ADD CONSTRAINT customers_name_phone_unique UNIQUE (customer_name, phone);
INSERT INTO car_shop.customers (customer_name, phone)
SELECT DISTINCT person_name, phone
FROM raw_data.sales
ON CONFLICT (customer_name, phone) DO NOTHING;
INSERT INTO car_shop.car_models (brand_id, model_name, base_gasoline_consumption)
SELECT 
    b.brand_id,
    CASE 
        WHEN s.auto LIKE 'Lada%' THEN SPLIT_PART(SPLIT_PART(s.auto, ', ', 1), ' ', 2)
        WHEN s.auto LIKE 'BMW%' THEN SPLIT_PART(SPLIT_PART(s.auto, ', ', 1), ' ', 2)
        WHEN s.auto LIKE 'Audi%' THEN SPLIT_PART(SPLIT_PART(s.auto, ', ', 1), ' ', 2)
        WHEN s.auto LIKE 'Tesla%' THEN CONCAT(SPLIT_PART(SPLIT_PART(s.auto, ', ', 1), ' ', 2), ' ', SPLIT_PART(SPLIT_PART(s.auto, ', ', 1), ' ', 3))
        WHEN s.auto LIKE 'Hyundai%' THEN SPLIT_PART(SPLIT_PART(s.auto, ', ', 1), ' ', 2)
        WHEN s.auto LIKE 'Kia%' THEN SPLIT_PART(SPLIT_PART(s.auto, ', ', 1), ' ', 2)
        WHEN s.auto LIKE 'Porsche%' THEN SPLIT_PART(SPLIT_PART(s.auto, ', ', 1), ' ', 2)
    END,
    AVG(CAST(NULLIF(gasoline_consumption, 'null') AS numeric))
FROM raw_data.sales s
JOIN car_shop.brands b ON 
    CASE 
        WHEN s.auto LIKE 'Lada%' THEN b.brand_name = 'Lada'
        WHEN s.auto LIKE 'BMW%' THEN b.brand_name = 'BMW'
        WHEN s.auto LIKE 'Audi%' THEN b.brand_name = 'Audi'
        WHEN s.auto LIKE 'Tesla%' THEN b.brand_name = 'Tesla'
        WHEN s.auto LIKE 'Hyundai%' THEN b.brand_name = 'Hyundai'
        WHEN s.auto LIKE 'Kia%' THEN b.brand_name = 'Kia'
        WHEN s.auto LIKE 'Porsche%' THEN b.brand_name = 'Porsche'
    END
GROUP BY b.brand_id, 
    CASE 
        WHEN s.auto LIKE 'Lada%' THEN SPLIT_PART(SPLIT_PART(s.auto, ', ', 1), ' ', 2)
        WHEN s.auto LIKE 'BMW%' THEN SPLIT_PART(SPLIT_PART(s.auto, ', ', 1), ' ', 2)
        WHEN s.auto LIKE 'Audi%' THEN SPLIT_PART(SPLIT_PART(s.auto, ', ', 1), ' ', 2)
        WHEN s.auto LIKE 'Tesla%' THEN CONCAT(SPLIT_PART(SPLIT_PART(s.auto, ', ', 1), ' ', 2), ' ', SPLIT_PART(SPLIT_PART(s.auto, ', ', 1), ' ', 3))
        WHEN s.auto LIKE 'Hyundai%' THEN SPLIT_PART(SPLIT_PART(s.auto, ', ', 1), ' ', 2)
        WHEN s.auto LIKE 'Kia%' THEN SPLIT_PART(SPLIT_PART(s.auto, ', ', 1), ' ', 2)
        WHEN s.auto LIKE 'Porsche%' THEN SPLIT_PART(SPLIT_PART(s.auto, ', ', 1), ' ', 2)
    END
ON CONFLICT (brand_id, model_name) DO NOTHING;
INSERT INTO car_shop.cars (car_id, model_id, price, sale_date, discount, customer_id)
SELECT 
    s.id,
    m.model_id,
    s.price,
    CAST(s.date AS date),  -- Явное преобразование в тип date
    s.discount,
    c.customer_id
FROM raw_data.sales s
JOIN car_shop.customers c ON s.person_name = c.customer_name AND s.phone = c.phone
JOIN car_shop.brands b ON 
    CASE 
        WHEN s.auto LIKE 'Lada%' THEN b.brand_name = 'Lada'
        WHEN s.auto LIKE 'BMW%' THEN b.brand_name = 'BMW'
        WHEN s.auto LIKE 'Audi%' THEN b.brand_name = 'Audi'
        WHEN s.auto LIKE 'Tesla%' THEN b.brand_name = 'Tesla'
        WHEN s.auto LIKE 'Hyundai%' THEN b.brand_name = 'Hyundai'
        WHEN s.auto LIKE 'Kia%' THEN b.brand_name = 'Kia'
        WHEN s.auto LIKE 'Porsche%' THEN b.brand_name = 'Porsche'
    END
JOIN car_shop.car_models m ON m.brand_id = b.brand_id AND 
    CASE 
        WHEN s.auto LIKE 'Lada%' THEN m.model_name = SPLIT_PART(SPLIT_PART(s.auto, ', ', 1), ' ', 2)
        WHEN s.auto LIKE 'BMW%' THEN m.model_name = SPLIT_PART(SPLIT_PART(s.auto, ', ', 1), ' ', 2)
        WHEN s.auto LIKE 'Audi%' THEN m.model_name = SPLIT_PART(SPLIT_PART(s.auto, ', ', 1), ' ', 2)
        WHEN s.auto LIKE 'Tesla%' THEN m.model_name = CONCAT(SPLIT_PART(SPLIT_PART(s.auto, ', ', 1), ' ', 2), ' ', SPLIT_PART(SPLIT_PART(s.auto, ', ', 1), ' ', 3))
        WHEN s.auto LIKE 'Hyundai%' THEN m.model_name = SPLIT_PART(SPLIT_PART(s.auto, ', ', 1), ' ', 2)
        WHEN s.auto LIKE 'Kia%' THEN m.model_name = SPLIT_PART(SPLIT_PART(s.auto, ', ', 1), ' ', 2)
        WHEN s.auto LIKE 'Porsche%' THEN m.model_name = SPLIT_PART(SPLIT_PART(s.auto, ', ', 1), ' ', 2)
    END;
    INSERT INTO car_shop.car_colors (car_id, color_id)
SELECT 
    s.id,
    c.color_id
FROM raw_data.sales s
CROSS JOIN LATERAL UNNEST(STRING_TO_ARRAY(SPLIT_PART(s.auto, ', ', 2), ', ')) AS color
JOIN car_shop.colors c ON TRIM(color) = c.color_name
ON CONFLICT (car_id, color_id) DO NOTHING;


-- Этап 2. Создание выборок

--Задание 1 из 6
SELECT 
    ROUND(COUNT(CASE WHEN base_gasoline_consumption IS NULL THEN 1 END) * 100.0 / 
          COUNT(*), 2) AS nulls_percentage_gasoline_consumption
FROM car_shop.car_models; --21.05
--Задание 2 из 6
SELECT 
    b.brand_name,
    EXTRACT(YEAR FROM c.sale_date) AS year,
    ROUND(AVG(c.price * (1 - COALESCE(c.discount, 0)/100)), 2) AS price_avg
FROM car_shop.cars c
JOIN car_shop.car_models m ON c.model_id = m.model_id
JOIN car_shop.brands b ON m.brand_id = b.brand_id
GROUP BY b.brand_name, EXTRACT(YEAR FROM c.sale_date)
ORDER BY b.brand_name, year;
--Задание 3 из 6
SELECT 
    EXTRACT(MONTH FROM sale_date) AS month,
    2022 AS year,
    ROUND(AVG(price * (1 - COALESCE(discount, 0)/100)), 2) AS price_avg
FROM car_shop.cars
WHERE EXTRACT(YEAR FROM sale_date) = 2022
GROUP BY EXTRACT(MONTH FROM sale_date)
ORDER BY month;
--Задание 4 из 6
SELECT 
    c.customer_name AS person,
    STRING_AGG(DISTINCT CONCAT(b.brand_name, ' ', m.model_name), ', ') AS cars
FROM car_shop.customers c
JOIN car_shop.cars cr ON c.customer_id = cr.customer_id
JOIN car_shop.car_models m ON cr.model_id = m.model_id
JOIN car_shop.brands b ON m.brand_id = b.brand_id
GROUP BY c.customer_name
ORDER BY c.customer_name;
--Задание 5 из 6
SELECT 
    b.origin_country AS brand_origin,
    MAX(c.price / (1 - COALESCE(c.discount, 0)/100)) AS price_max,
    MIN(c.price / (1 - COALESCE(c.discount, 0)/100)) AS price_min
FROM car_shop.cars c
JOIN car_shop.car_models m ON c.model_id = m.model_id
JOIN car_shop.brands b ON m.brand_id = b.brand_id
GROUP BY b.origin_country;
--Задание 6 из 6
SELECT 
    COUNT(*) AS persons_from_usa_count
FROM car_shop.customers
WHERE phone LIKE '+1%'; --131




