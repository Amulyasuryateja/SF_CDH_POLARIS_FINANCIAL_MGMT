{{
    config(
        tags=["SC_VW","sit1"]
    )
}}

/*  CTE to fetch data from source object dim_budget_profile */
with dim_budget_profile as (

    select
        current_audit_datetime,
        source_system,
        budget_profile,
        client,
        category_name,
        budget_type,
        stage_id,
        stage_source_table,
        source_audit_datetime,
        sk_dim_budget_profile
    from {{ ref("dim_budget_profile") }}
),

with fct_sales as (
    select
        sk_fct_sales_transaction,
        transaction_id,
        transaction_date,
        customer_id,
        product_id,
        sales_amount,
        quantity_sold,
        discount_amount,
        sales_channel,
        is_returned_flag,
        current_audit_timestamp as fct_audit_timestamp
    from {{ ref('fct_sales_transaction') }}
),

/*  CTE to create a model from dim_budget_profile as per ADS document */
final as (

    select
        greatest(
           coalesce(a.current_audit_datetime, cast('1900-01-01' as datetime)),
           coalesce(b.current_audit_datetime, cast('1900-01-01' as datetime))
        ) as dp_timestamp,
        a.source_system as source_system,
        a.budget_profile as budget_profile,
        a.client as client,
        a.category_name as category_name,
        a.budget_type as budget_type,
        a.stage_id as stage_id,
        a.stage_source_table as stage_source_table,
        a.source_audit_datetime as source_audit_datetime
    from dim_budget_profile as a
    where a.sk_dim_budget_profile <> '-1'

)

select
    dp_timestamp,
    source_system,
    budget_profile,
    client,
    category_name,
    budget_type,
    stage_id,
    stage_source_table,
    source_audit_datetime
from final
