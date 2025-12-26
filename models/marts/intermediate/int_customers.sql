with customers as (
    select * from {{ ref ('stg_customers') }}
),

orders as (
    select * from {{ ref ('int_orders') }}
),

customer_orders as (

    select
        orders.*,
        customers.full_name,
        customers.surname,
        customers.givenname,

    --customers level aggregations

        min(order_date) over (
            partition by orders.customer_id
        ) as customer_first_order_date,

        min(valid_order_date) over (
            partition by orders.customer_id
        ) as customer_first_non_returned_order_date,

        max(valid_order_date) over (
            partition by orders.customer_id
        ) as customer_most_recent_non_returned_order_date,

        count(*) over(
            partition by orders.customer_id
        ) as customer_order_count,

        sum(
            if(valid_order_date is not null,1,0)
        ) over(
            partition by orders.customer_id
        ) as customer_non_returned_order_count,

        sum(
            if(valid_order_date is not null,order_value_dollars,0)
        ) over(
            partition by orders.customer_id
        ) as customer_total_lifetime_value,

        array_agg(order_id) 
        over(
            partition by orders.customer_id
        ) as customer_order_ids

    from orders
    join customers
    on customers.customer_id = orders.customer_id
),

customer_average_order_values as (   select
        *,
        {{ function('safe_divide') }}(customer_total_lifetime_value, customer_non_returned_order_count)
        as customer_avg_non_returned_order_value

    from customer_orders
)

select 
*
from customer_average_order_values