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
        order_id, 
        any_value(order_date) as order_placed_at,
        any_value(customer_id) as customer_id,
        any_value(order_status) as order_status,
        max(created_date) as payment_finalized_date, 
        sum(amount) as total_amount_paid
    from payments
    left join orders using (order_id)
    where payment_status <> 'fail'
    group by 1

),

customer_orders as (

    select
        customer_id,
        any_value(customer_first_name) as customer_first_name,
        any_value(customer_last_name) as customer_last_name,
        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as number_of_orders
    from customers
    left join orders using (customer_id)
    group by 1

),

cume_lifetime_value as (

    select
        customer_id,
        order_id,
        sum(total_amount_paid) over(partition by customer_id order by order_id) as customer_lifetime_value
    from paid_orders

),

final as (

    select 
        po.order_id,
        customer_id,
        order_placed_at,
        order_status,
        total_amount_paid,
        payment_finalized_date,
        customer_first_name,
        customer_last_name,
        row_number() over (order by po.order_id) as transaction_seq,
        row_number() over (partition by customer_id order by po.order_id) as customer_sales_seq,
        case
        when first_order_date = order_placed_at then 'new'
            else 'return'
        end as nvsr,
        customer_lifetime_value,
        first_order_date as fdos
    from paid_orders as po
    join customer_orders as co using (customer_id)
    join cume_lifetime_value as lv using (customer_id, order_id)
    order by order_id

)

select * from final

