-- models/marts/dm_customer_order_ranking.sql

-- 1. Выбираем все заказы со статусом 'завершен'
with completed_orders as (
    select
        order_pk
    from {{ ref('sat_order') }}
    where status = 'completed'
),

-- 2. Считаем количество завершенных заказов для каждого клиента
customer_order_counts as (
    select
        lco.customer_pk,
        count(co.order_pk) as completed_orders_count
    from completed_orders as co
    inner join {{ ref('link_customer_order') }} as lco
        on co.order_pk = lco.order_pk
    group by
        lco.customer_pk
),

-- 3. Получаем самые актуальные имена клиентов
latest_customer_details as (
    select
        customer_pk,
        first_name,
        last_name
    from (
        select
            customer_pk,
            first_name,
            last_name,
            -- Нумеруем записи для каждого клиента по дате в обратном порядке, чтобы найти самую свежую
            row_number() over(partition by customer_pk order by effective_from desc) as rn
        from {{ ref('sat_customer') }}
    ) as ranked_customers
    where rn = 1
)

-- 4. Соединяем все вместе и сортируем
select
    lcd.first_name,
    lcd.last_name,
    coc.completed_orders_count
from customer_order_counts as coc
inner join latest_customer_details as lcd
    on coc.customer_pk = lcd.customer_pk
order by
    coc.completed_orders_count desc
