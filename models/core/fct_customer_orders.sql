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
        when rank() over(partition by customer_id order by order_placed_at) = 1 then 'new'
            else 'return'
        end as nvsr,
        sum(total_amount_paid) over(partition by customer_id order by order_id) as customer_lifetime_value,
        min(order_placed_at) over(partition by customer_id order by order_placed_at) as fdos
    from paid_orders as po
    join customers using (customer_id)
    order by order_id

)

select * from final

