{{
    config(
        tags=["SC_VW"]
    )
}}

/*  CTE to fetch data from source object fct_revenue_records */
with fct_revenue_records as (

    select
        current_audit_datetime,
        source_system,
        revenue_record_id,
        product_id,
        revenue_date,
        revenue_amount,
        revenue_currency,
        revenue_channel,
        revenue_status,
        audit_id,
        source_audit_datetime,
        sk_fact_revenue_record
    from {{ ref("fct_revenue_records") }}
),

dim_vendor as (
    select
        sk_dim_vendor_master,
        vendor_master,
        region,
        group_name,
        type_id
    from {{ ref('dim_vendor_master') }}
),

/*  CTE to create a model from fct_revenue_records as per ADS document */
final as (

    select
        greatest(
           coalesce(a.current_audit_datetime, cast('1900-01-01' as datetime)),
           coalesce(b.current_audit_datetime, cast('1900-01-01' as datetime))
        ) as dp_timestamp,
        a.source_system as source_system,
        a.revenue_record_id as revenue_record_id,
        a.product_id as product_id,
        a.revenue_date as revenue_date,
        a.revenue_amount as revenue_amount,
        a.revenue_currency as revenue_currency,
        a.revenue_channel as revenue_channel,
        a.revenue_status as revenue_status,
        a.audit_id as audit_id,
        a.source_audit_datetime as source_audit_datetime
    from fct_revenue_records as a
    where a.sk_fact_revenue_record <> '-1'

)

select
    dp_timestamp,
    source_system,
    revenue_record_id,
    product_id,
    revenue_date,
    revenue_amount,
    revenue_currency,
    revenue_channel,
    revenue_status,
    audit_id,
    source_audit_datetime
from final
