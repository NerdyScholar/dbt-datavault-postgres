-- models/marts/weekly_order_dynamics.sql

with source_data as (
    -- Выбираем первичный ключ заказа и дату заказа из сателлита
    select
        order_pk,
        order_date
    from {{ ref('sat_order') }}
)

select
    -- Обрезаем дату до начала недели (понедельника)
    date_trunc('week', order_date)::date as week_start_date,

    -- Считаем количество уникальных заказов за неделю.
    -- Использование distinct важно, так как в сателлиite могут быть
    -- несколько записей для одного заказа, если его атрибуты менялись.
    count(distinct order_pk) as total_orders
from source_data
group by 1
order by 1