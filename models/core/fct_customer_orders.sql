with orders as (

    select * from {{ ref('stg_jaffle_shop__orders') }}

),

payments as (

    select * from {{ ref('stg_stripe__payments') }}

),

customers as (

    select * from {{ ref('stg_jaffle_shop__customers') }}

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
    left join orders using (order_id)
    where payment_status <> 'fail'
    group by 1

),

final as (

    select 
        paid_orders.order_id,
        paid_orders.customer_id,
        paid_orders.order_placed_at,
        paid_orders.order_status,
        paid_orders.total_amount_paid,
        paid_orders.payment_finalized_date,
        customers.customer_first_name,
        customers.customer_last_name,
        row_number() over (order by paid_orders.order_id) as transaction_seq,
        row_number() over (partition by paid_orders.customer_id order by paid_orders.order_id) as customer_sales_seq,
        case
        when rank() over(partition by paid_orders.customer_id order by paid_orders.order_placed_at) = 1 then 'new'
            else 'return'
        end as nvsr,
        sum(paid_orders.total_amount_paid) over(partition by paid_orders.customer_id order by paid_orders.order_id) as customer_lifetime_value,
        min(paid_orders.order_placed_at) over(partition by paid_orders.customer_id order by paid_orders.order_placed_at) as fdos
    from paid_orders
    join customers using (customer_id)
    order by order_id

)

select * from final

