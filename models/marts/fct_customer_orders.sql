-- Import CTEs

with customer_orders as (
    select *
    from {{ref ('int_customers')}}
),

-- Final CTEs

final as (
    select

        order_id,
        customer_id,
        surname,
        givenname,
        customer_first_order_date as first_order_date,
        customer_order_count as order_count,
        customer_total_lifetime_value as total_lifetime_value,
        order_value_dollars, -- pay attention to this during audit
        order_status,
        payment_status

    from customer_orders

     )
  
    -- Final Select

    select
    *
    from final