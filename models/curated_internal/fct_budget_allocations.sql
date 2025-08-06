{{
config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key="sk_fct_budget_allocation",
    tags=["SC_FCT"]
)
}}

with source as (

    select
        budget_system,
        allocation_id,
        budget_period,
        department_id,
        allocated_amount,
        allocation_status,
        budget_code,
        allocation_audit_id,
        allocation_audit_time as src_audit_time

    from {{ source("sap_erp","cobl_hist") }}

    {% if is_incremental() %}
        changes (information => append_only) at(timestamp => '{{ dbt_xom_package.get_incremental_ts(this) }}'::timestamp_tz)
        where
            metadata$action = 'INSERT'
            and src_audit_time > '{{ dbt_xom_package.get_incremental_ts(this)}}'::timestamp_tz
            and budget_system in ('P4')
        qualify 1 row_number() over(
            partition by budget_system, allocation_id order by "header_timestamp" desc
        )
    {% else %}
        where
            is_active_flag = true and budget_system in ('P4')
    {% endif %}

),

final as (

    select
        ifnull(
            {{
                dbt_utils.generate_surrogate_key([
                    'src.budget_system', 'src.allocation_id'
                ])
            }}, '-1'
        ) as sk_fct_budget_allocation,
        src.allocation_id,
        src.department_id,
        src.budget_period,
        src.allocated_amount,
        src.allocation_status,
        src.budget_code,
        src.budget_system as source_system,
        src.allocation_audit_id as audit_id,
        'COBL_HIST' as stage_source_table,
        src.src_audit_time as source_audit_datetime,
        current_timestamp() as current_audit_datetime

    from source as src

)

select
    sk_fct_budget_allocation,
    allocation_id,
    department_id,
    budget_period,
    allocated_amount,
    allocation_status,
    budget_code,
    source_system,
    audit_id,
    stage_source_table,
    source_audit_datetime,
    current_audit_datetime

from final
