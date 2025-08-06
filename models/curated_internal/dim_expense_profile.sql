{{

config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key="sk_dim_expense_profile",
    tags=["SC_DIM"]
)

}}

with source as (

    select
        expense_system,
        business_unit,
        expense_name,
        expense_class,
        expense_type_id,
        expense_delete_flag,
        expense_audit_id,
        expense_audit_time as src_audit_time

    from {{ source("sap_erp","lfbk_hist") }}

    {% if is_incremental() %}
        changes (information => append_only) at(timestamp => '{{ dbt_xom_package.get_incremental_ts(this) }}'::timestamp_tz)
        where
            metadata$action = 'INSERT'
            and src_audit_time > '{{ dbt_xom_package.get_incremental_ts(this)}}'::timestamp_tz
            and expense_system in ('P4')
        qualify 1 row_number() over(
            partition by expense_system, business_unit, expense_name order by "header_timestamp" desc
        )
    {% else %}
        where
            is_latest_flag = true and expense_system in ('P4')
    {% endif %}

),

final as (

    select
        ifnull(
            {{
                dbt_utils.generate_surrogate_key([
                    'src.expense_system', 'src.business_unit', 'src.expense_name'
                ])
            }}, '-1'
        ) as sk_dim_expense_profile,
        src.expense_name as expense_profile,
        src.expense_delete_flag as delete_flag,
        src.expense_system as source_system,
        src.business_unit as client,
        src.expense_type_id as expense_type,
        current_timestamp() as current_audit_datetime,
        src.expense_class as class_name,
        src.expense_audit_id as stage_id,
        'LFBK_HIST' as stage_source_table,
        src.src_audit_time as source_audit_datetime

    from source as src

)

select
    sk_dim_expense_profile,
    delete_flag,
    source_system,
    client,
    expense_profile,
    class_name,
    expense_type,
    current_audit_datetime,
    stage_id,
    stage_source_table,
    source_audit_datetime

from final
