{{

config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key="sk_dim_revenue_profile",
    tags=["SC_DIM"]
)

}}

with source as (

    select
        revenue_system,
        division_id,
        revenue_name,
        revenue_class,
        revenue_type_id,
        revenue_delete_flag,
        revenue_audit_id,
        revenue_audit_time as src_audit_time

    from {{ source("sap_erp","skat_hist") }}

    {% if is_incremental() %}
        changes (information => append_only) at(timestamp => '{{ dbt_xom_package.get_incremental_ts(this) }}'::timestamp_tz)
        where
            metadata$action = 'INSERT'
            and src_audit_time > '{{ dbt_xom_package.get_incremental_ts(this)}}'::timestamp_tz
            and revenue_system in ('P4')
        qualify 1 row_number() over(
            partition by revenue_system, division_id, revenue_name order by "header_timestamp" desc
        )
    {% else %}
        where
            is_valid_flag = true and revenue_system in ('P4')
    {% endif %}

),

final as (

    select
        ifnull(
            {{
                dbt_utils.generate_surrogate_key([
                    'src.revenue_system', 'src.division_id', 'src.revenue_name'
                ])
            }}, '-1'
        ) as sk_dim_revenue_profile,
        src.revenue_name as revenue_profile,
        src.revenue_delete_flag as delete_flag,
        src.revenue_system as source_system,
        src.division_id as client,
        src.revenue_type_id as revenue_type,
        current_timestamp() as current_audit_datetime,
        src.revenue_class as class_name,
        src.revenue_audit_id as stage_id,
        'SKAT_HIST' as stage_source_table,
        src.src_audit_time as source_audit_datetime

    from source as src

)

select
    sk_dim_revenue_profile,
    delete_flag,
    source_system,
    client,
    revenue_profile,
    class_name,
    revenue_type,
    current_audit_datetime,
    stage_id,
    stage_source_table,
    source_audit_datetime

from final
