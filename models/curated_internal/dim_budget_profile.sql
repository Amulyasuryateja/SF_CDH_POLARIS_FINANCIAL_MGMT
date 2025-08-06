{{

config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key="sk_dim_budget_profile",
    tags=["SC_DIM"]
)

}}

with source as (

    select
        ledger_system,
        company_code,
        budget_name,
        budget_category,
        budget_type_id,
        soft_delete_flag,
        budget_audit_id,
        budget_audit_time as src_audit_time

    from {{ source("sap_erp","bkpf_hist") }}

    {% if is_incremental() %}
        changes (information => append_only) at(timestamp => '{{ dbt_xom_package.get_incremental_ts(this) }}'::timestamp_tz)
        where
            metadata$action = 'INSERT'
            and src_audit_time > '{{ dbt_xom_package.get_incremental_ts(this)}}'::timestamp_tz
            and ledger_system in ('P4')
        qualify 1 row_number() over(
            partition by ledger_system, company_code, budget_name order by "header_timestamp" desc
        )
    {% else %}
        where
            is_active_flag = true and ledger_system in ('P4')
    {% endif %}

),

final as (

    select
        ifnull(
            {{
                dbt_utils.generate_surrogate_key([
                    'src.ledger_system', 'src.company_code', 'src.budget_name'
                ])
            }}, '-1'
        ) as sk_dim_budget_profile,
        src.budget_name as budget_profile,
        src.soft_delete_flag as delete_flag,
        src.ledger_system as source_system,
        src.company_code as client,
        src.budget_type_id as budget_type,
        current_timestamp() as current_audit_datetime,
        src.budget_category as category_name,
        src.budget_audit_id as stage_id,
        'BKPF_HIST' as stage_source_table,
        src.src_audit_time as source_audit_datetime

    from source as src

)

select
    sk_dim_budget_profile,
    delete_flag,
    source_system,
    client,
    budget_profile,
    category_name,
    budget_type,
    current_audit_datetime,
    stage_id,
    stage_source_table,
    source_audit_datetime

from final
