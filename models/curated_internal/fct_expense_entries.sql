{{
config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key="sk_fct_expense_entry",
    tags=["SC_fct"]
)
}}

with source as (

    select
        expense_system,
        expense_entry_id,
        expense_date,
        employee_id,
        expense_category,
        expense_amount,
        expense_currency,
        expense_status,
        expense_audit_id,
        expense_audit_time as src_audit_time

    from {{ source("sap_erp","ekko_hist") }}

    {% if is_incremental() %}
        changes (information => append_only) at(timestamp => '{{ dbt_xom_package.get_incremental_ts(this) }}'::timestamp_tz)
        where
            metadata$action = 'INSERT'
            and src_audit_time > '{{ dbt_xom_package.get_incremental_ts(this)}}'::timestamp_tz
            and expense_system in ('P4')
        qualify 1 row_number() over(
            partition by expense_system, expense_entry_id order by "header_timestamp" desc
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
                    'src.expense_system', 'src.expense_entry_id'
                ])
            }}, '-1'
        ) as sk_fct_expense_entry,

        src.expense_entry_id,
        src.employee_id,
        src.expense_date,
        src.expense_category,
        src.expense_amount,
        src.expense_currency,
        src.expense_status,
        src.expense_system as source_system,
        src.expense_audit_id as audit_id,
        'EKKO_HIST' as stage_source_table,
        src.src_audit_time as source_audit_datetime,
        current_timestamp() as current_audit_datetime

    from source as src

)

select
    sk_fct_expense_entry,
    expense_entry_id,
    employee_id,
    expense_date,
    expense_category,
    expense_amount,
    expense_currency,
    expense_status,
    source_system,
    audit_id,
    stage_source_table,
    source_audit_datetime,
    current_audit_datetime

from final
