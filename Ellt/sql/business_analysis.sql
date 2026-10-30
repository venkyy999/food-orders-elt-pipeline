USE food_orders_analytics_db;

-- Order-value totals and averages include only rows with a non-NULL Order Value.
SELECT
    COUNT(`Order Value`) AS records_included,
    SUM(`Order Value`) AS total_order_value,
    AVG(`Order Value`) AS average_order_value
FROM cleaned_food_orders;

-- NULL payment methods are excluded from the category ranking.
SELECT `Payment Method`, COUNT(*) AS order_count
FROM cleaned_food_orders
WHERE `Payment Method` IS NOT NULL
GROUP BY `Payment Method`
ORDER BY order_count DESC, `Payment Method`
LIMIT 1;

-- NULL order channels are excluded from the category ranking.
SELECT `Order Channel`, COUNT(*) AS order_count
FROM cleaned_food_orders
WHERE `Order Channel` IS NOT NULL
GROUP BY `Order Channel`
ORDER BY order_count DESC, `Order Channel`
LIMIT 1;

-- Accepted records only; rejected negative or invalid durations are not loaded.
SELECT
    COUNT(Delivery_Time_Minutes) AS records_included,
    AVG(Delivery_Time_Minutes) AS average_delivery_time_minutes
FROM cleaned_food_orders;

-- Revenue is NULL unless all four components were present. SUM ignores those NULLs.
SELECT
    COUNT(Platform_Net_Revenue) AS records_included,
    SUM(Platform_Net_Revenue) AS total_platform_net_revenue
FROM cleaned_food_orders;
