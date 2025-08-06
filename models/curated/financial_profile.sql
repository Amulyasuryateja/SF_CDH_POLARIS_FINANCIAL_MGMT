{{
    config(
        tags=["SC_VW","sit1"]
    )
}}

/*  CTE to fetch data from source object dim_financial_profile */
with dim_financial_profile as (

    select
        current_audit_datetime,
        source_system,
        financial_profile,
        client,
        class_name,
        account_type,
        stage_id,
        stage_source_table,
        source_audit_datetime,
        sk_dim_financial_profile
    from {{ ref("dim_financial_profile") }}
),

dim_employee as (
    select
        sk_dim_employee_record,
        employee_record,
        department,
        role,
        role_type
    from {{ ref('dim_employee_record') }}
),

/*  CTE to create a model from dim_financial_profile as per ADS document */
final as (

    select
        greatest(a.current_audit_datetime) as dp_timestamp,
        a.source_system as source_system,
        a.financial_profile as financial_profile,
        a.client as client,
        a.class_name as class_name,
        a.account_type as account_type,
        a.stage_id as stage_id,
        a.stage_source_table as stage_source_table,
        a.source_audit_datetime as source_audit_datetime
    from dim_financial_profile as a
    where a.sk_dim_financial_profile <> '-1'

)

select
    dp_timestamp,
    source_system,
    financial_profile,
    client,
    class_name,
    account_type,
    stage_id,
    stage_source_table,
    source_audit_datetime
from final
