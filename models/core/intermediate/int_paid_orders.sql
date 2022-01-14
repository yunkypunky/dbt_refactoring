with orders as (

    select * from {{ ref('stg_jaffle_shop__orders') }}

),

payments as (

    select * from {{ ref('stg_stripe__payments') }}

),

paid_orders as (

    select 
        payments.order_id, 
        any_value(orders.order_date) as order_placed_at,
        any_value(orders.customer_id) as customer_id,
        any_value(orders.order_status) as order_status,
        max(payments.created_date) as payment_finalized_date, 
        sum(payments.amount) as total_amount_paid
    from payments
    left join orders 
        on (payments.order_id = orders.order_id)
    where payment_status <> 'fail'
    group by 1

)

select * from paid_orders