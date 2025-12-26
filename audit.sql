--------------------------------------------------
--- Compare Row Counts
--------------------------------------------------

{% set old_relation = adapter.get_relation(
      database = target.database,
      schema = "dbt_oba",
      identifier = "customer_orders_legacy"
) -%}

{% set dbt_relation = ref('fct_customer_orders') %}

{{ audit_helper.compare_row_counts(
    a_relation = old_relation,
    b_relation = dbt_relation
) }}

--legacy : 113
-- refactored 99
--------------------------------------------------
--- Compare all columns
--------------------------------------------------



{% set old_relation = adapter.get_relation(
      database = target.database,
      schema = "dbt_oba",
      identifier = "customer_orders_legacy"
) -%}

{% set dbt_relation = ref('fct_customer_orders') %}

{% if execute %}

{{ audit_helper.compare_all_columns(
    a_relation = old_relation,
    b_relation = dbt_relation,
    primary_key = "order_id"
) }}

{% endif %}
