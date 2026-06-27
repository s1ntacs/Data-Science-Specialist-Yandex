/* Проект «Разработка витрины и решение ad-hoc задач»
 * Цель проекта: подготовка витрины данных маркетплейса «ВсёТут»
 * и решение четырех ad hoc задач на её основе
 * 
 * Автор: Aleksander Stepanov
 * Дата: 
*/

WITH
-- 1. Фильтрация заказов: только "Доставлено" и "Отменено"
filtered_orders AS (
    SELECT 
        o.order_id,
        o.buyer_id,
        o.order_status,
        o.order_purchase_ts,
        u.user_id,
        u.region
    FROM ds_ecom.orders o
    JOIN ds_ecom.users u ON o.buyer_id = u.buyer_id
    WHERE o.order_status IN ('Доставлено', 'Отменено')
),

-- 2. Топ-3 региона по количеству заказов
top_regions AS (
    SELECT region
    FROM filtered_orders
    GROUP BY region
    ORDER BY COUNT(order_id) DESC
    LIMIT 3
),

order_ratings_agg AS (
    SELECT 
        r.order_id,
        AVG(
            CASE 
                WHEN r.review_score > 5 THEN r.review_score / 10.0
                ELSE r.review_score
            END
        ) AS avg_normalized_score
    FROM ds_ecom.order_reviews r
    GROUP BY r.order_id
),

-- наличие промокода и рассрочки
payments_agg AS (
    SELECT 
        op.order_id,
        MIN(op.payment_type) FILTER (WHERE op.payment_sequential = 1) AS first_payment_type,
        MAX(CASE WHEN op.payment_type = 'промокод' THEN 1 ELSE 0 END) AS used_promo,
        MAX(CASE WHEN op.payment_installments > 1 THEN 1 ELSE 0 END) AS used_installment
    FROM ds_ecom.order_payments op
    GROUP BY op.order_id
),


order_costs_agg AS (
    SELECT 
        oi.order_id,
        SUM(oi.price + oi.delivery_cost) AS total_cost
    FROM ds_ecom.order_items oi
    JOIN ds_ecom.orders o ON oi.order_id = o.order_id
    WHERE o.order_status = 'Доставлено'
    GROUP BY oi.order_id
)

SELECT
    fo.user_id,
    fo.region,
    MIN(fo.order_purchase_ts) AS first_order_ts,
    MAX(fo.order_purchase_ts) AS last_order_ts,
    DATE_PART('day', MAX(fo.order_purchase_ts) - MIN(fo.order_purchase_ts)) AS lifetime,
    COUNT(fo.order_id) AS total_orders,
    ROUND(AVG(r.avg_normalized_score), 2) AS avg_order_rating,
    COUNT(r.order_id) AS num_orders_with_rating,
    COUNT(*) FILTER (WHERE fo.order_status = 'Отменено') AS num_canceled_orders,
    ROUND(
        COUNT(*) FILTER (WHERE fo.order_status = 'Отменено')::NUMERIC / COUNT(*),
        4
    ) AS canceled_orders_ratio,
    SUM(oc.total_cost) AS total_order_costs,
    ROUND(AVG(oc.total_cost), 2) AS avg_order_cost,
    SUM(p.used_installment) AS num_installment_orders,
    SUM(p.used_promo) AS num_orders_with_promo,
    MAX(CASE WHEN p.first_payment_type = 'денежный перевод' THEN 1 ELSE 0 END) AS used_money_transfer,
    MAX(p.used_installment) AS used_installments,
    MAX(CASE WHEN fo.order_status = 'Отменено' THEN 1 ELSE 0 END) AS used_cancel
FROM filtered_orders fo
JOIN top_regions tr ON fo.region = tr.region
LEFT JOIN order_ratings_agg r ON fo.order_id = r.order_id
LEFT JOIN payments_agg p ON fo.order_id = p.order_id
LEFT JOIN order_costs_agg oc ON fo.order_id = oc.order_id
GROUP BY fo.user_id, fo.region
ORDER BY fo.user_id, fo.region;


/* Часть 2. Решение ad hoc задач
 * Для каждой задачи напишите отдельный запрос.
 * После каждой задачи оставьте краткий комментарий с выводами по полученным результатам.
*/

/* Задача 1. Сегментация пользователей 
 * Разделите пользователей на группы по количеству совершённых ими заказов.
 * Подсчитайте для каждой группы общее количество пользователей,
 * среднее количество заказов, среднюю стоимость заказа.
 * 
 * 
 * Выделите такие сегменты:
 * - 1 заказ — сегмент 1 заказ
 * - от 2 до 5 заказов — сегмент 2-5 заказов
 * - от 6 до 10 заказов — сегмент 6-10 заказов
 * - 11 и более заказов — сегмент 11 и более заказов
*/


-- Напишите ваш запрос тут
-- Задача 1: Сегментация пользователей по количеству заказов
SELECT 
    segment,
    COUNT(*) AS users_count,
    ROUND(AVG(total_orders), 2) AS avg_orders_per_user,
    ROUND(AVG(avg_order_cost), 2) AS avg_order_cost
FROM (
    SELECT 
        CASE 
            WHEN total_orders = 1 THEN '1 заказ'
            WHEN total_orders BETWEEN 2 AND 5 THEN '2—5 заказов'
            WHEN total_orders BETWEEN 6 AND 10 THEN '6—10 заказов'
            ELSE '11 и более заказов'
        END AS segment,
        total_orders,
        avg_order_cost
    FROM ds_ecom.product_user_features
) AS seg
GROUP BY segment
ORDER BY 
    CASE segment
        WHEN '1 заказ' THEN 1
        WHEN '2—5 заказов' THEN 2
        WHEN '6—10 заказов' THEN 3
        ELSE 4
    END;

/* Напишите краткий комментарий с выводами по результатам задачи 1.
 * Большинство пользователей — новички (60 468 человек), совершающие только один заказ.
 *Это говорит о высокой конверсии в первую покупку, но низком удержании.
Пользователи с 2–5 заказами — это основная масса лояльных клиентов (1 934 человека), 
у которых средний чек (3 091 руб.) немного ниже, 
чем у новичков возможно, они делают более частые, но менее дорогие покупки.
Сегменты 6–10 и 11+ заказов крайне малочисленны (всего 6 человек), 
но демонстрируют наиболее высокую активность и средний чек (до 1 244,8 тыс руб).
*/



/* Задача 2. Ранжирование пользователей 
 * Отсортируйте пользователей, сделавших 3 заказа и более, по убыванию среднего чека покупки.  
 * Выведите 15 пользователей с самым большим средним чеком среди указанной группы.
*/

-- Напишите ваш запрос тут
SELECT 
    user_id,
    region,
    total_orders,
    avg_order_cost
FROM ds_ecom.product_user_features
WHERE total_orders >= 3
ORDER BY avg_order_cost DESC
LIMIT 15;

/* Напишите краткий комментарий с выводами по результатам задачи 2.
 * В топ-15 вошли пользователи из Москвы, Санкт-Петербурга и Новосибирской области, 
 * что подтверждает доминирующую роль крупных городов в высокодоходной аудитории.
Лидер пользователь из Санкт-Петербурга с средним чеком 14 716 руб, 
что указывает на очень высокую покупательскую способность.
У большинства пользователей — 3–5 заказов, но при этом средний чек колеблется от 5 500 до 14 700 руб, что говорит о том, 
что повторная покупка не всегда связана с частотой, а зависит от стоимости товаров.
*/



/* Задача 3. Статистика по регионам. 
 * Для каждого региона подсчитайте:
 * - общее число клиентов и заказов;
 * - среднюю стоимость одного заказа;
 * - долю заказов, которые были куплены в рассрочку;
 * - долю заказов, которые были куплены с использованием промокодов;
 * - долю пользователей, совершивших отмену заказа хотя бы один раз.
*/
SELECT 
    region,
    COUNT(*) AS total_users,
    SUM(total_orders) AS total_orders,
    AVG(avg_order_cost) AS avg_order_cost,
    SUM(num_installment_orders)::FLOAT / SUM(total_orders) AS installment_orders_ratio,
    SUM(num_orders_with_promo)::FLOAT / SUM(total_orders) AS promo_orders_ratio,
    AVG(used_cancel) AS cancel_users_ratio
FROM ds_ecom.product_user_features
GROUP BY region
ORDER BY total_users DESC;

-- Напишите ваш запрос тут

/* Напишите краткий комментарий с выводами по результатам задачи 3.
 * Москва абсолютный лидер: 39 386 пользователей, 
 * 40 747 заказов и средний чек 3 167 руб. 
 * Санкт-Петербург второй по активности: 11 978 пользователей, средний чек 3 620 руб выше, чем в Москве, 
 * что указывает на более высокую покупательскую способность.
Новосибирская область третья: 11 044 пользователя, средний чек 3 519 руб почти как в СПб.
*/



/* Задача 4. Активность пользователей по первому месяцу заказа в 2023 году
 * Разбейте пользователей на группы в зависимости от того, в какой месяц 2023 года они совершили первый заказ.
 * Для каждой группы посчитайте:
 * - общее количество клиентов, число заказов и среднюю стоимость одного заказа;
 * - средний рейтинг заказа;
 * - долю пользователей, использующих денежные переводы при оплате;
 * - среднюю продолжительность активности пользователя.
*/

-- Напишите ваш запрос тут
SELECT 
    EXTRACT(MONTH FROM first_order_ts) AS month_2023,
    COUNT(*) AS users_count,
    SUM(total_orders) AS total_orders,
    AVG(avg_order_cost) AS avg_order_cost,
    AVG(avg_order_rating) AS avg_rating,
    AVG(used_money_transfer) AS money_transfer_ratio,
    AVG(lifetime) AS avg_lifetime_days
FROM ds_ecom.product_user_features
WHERE EXTRACT(YEAR FROM first_order_ts) = 2023
GROUP BY month_2023
ORDER BY month_2023;
/* Напишите краткий комментарий с выводами по результатам задачи 4.
 * Пик активности новых пользователей пришёлся на декабрь (3 589 человек) и ноябрь (4 703 человека) это сезонный пик, связанный с подготовкой к Новому году, праздниками и акциями.
В июне (2 197 человек) и сентябре (2 591 человек) 
также наблюдается рост,возможно, связано с летними отпусками и началом учебного года.
 * 
