{{
config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key="sk_fct_financial_transaction",
    tags=["SC_FCT"]
)
}}

with source as (

    select
        transaction_system,
        transaction_id,
        transaction_date,
        account_id,
        amount_local,
        currency_code,
        transaction_type,
        approval_status,
        transaction_audit_id,
        transaction_audit_time as src_audit_time

    from {{ source("sap_erp","bpeg_hist") }}

    {% if is_incremental() %}
        changes (information => append_only) at(timestamp => '{{ dbt_xom_package.get_incremental_ts(this) }}'::timestamp_tz)
        where
            metadata$action = 'INSERT'
            and src_audit_time > '{{ dbt_xom_package.get_incremental_ts(this)}}'::timestamp_tz
            and transaction_system in ('P4')
        qualify 1 row_number() over(
            partition by transaction_system, transaction_id order by "header_timestamp" desc
        )
    {% else %}
        where
            is_current_flag = true and transaction_system in ('P4')
    {% endif %}

),

final as (

    select
        ifnull(
            {{
                dbt_utils.generate_surrogate_key([
                    'src.transaction_system', 'src.transaction_id'
                ])
            }}, '-1'
        ) as sk_fct_financial_transaction,

        src.transaction_id,
        src.account_id,
        src.transaction_date,
        src.amount_local,
        src.currency_code,
        src.transaction_type,
        src.approval_status,
        src.transaction_system as source_system,
        src.transaction_audit_id as audit_id,
        'BPEG_HIST' as stage_source_table,
        src.src_audit_time as source_audit_datetime,
        current_timestamp() as current_audit_datetime

    from source as src

)

select
    sk_fct_financial_transaction,
    transaction_id,
    account_id,
    transaction_date,
    amount_local,
    currency_code,
    transaction_type,
    approval_status,
    source_system,
    audit_id,
    stage_source_table,
    source_audit_datetime,
    current_audit_datetime

from final
