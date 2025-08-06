{{

config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key="sk_dim_financial_profile",
    tags=["SC_DIM"]
)

}}

with source as (

    select
        src_system,
        client_id,
        account_name,
        account_class,
        account_type_id,
        is_deleted_flag,
        audit_id,
        audit_timestamp as src_audit_timestamp

    from {{ source("sap_erp","bsak_hist") }}

    {% if is_incremental() %}
        changes (information => append_only) at(timestamp => '{{ dbt_xom_package.get_incremental_ts(this) }}'::timestamp_tz)
        where
            metadata$action = 'INSERT'
            and src_audit_timestamp > '{{ dbt_xom_package.get_incremental_ts(this)}}'::timestamp_tz
            and src_system in ('P4')
        qualify 1 row_number() over(
            partition by src_system, client_id, account_name order by "header_timestamp" desc
        )
    {% else %}
        where
            is_current_flag = true and src_system in ('P4')
    {% endif %}

),

final as (

    select
        ifnull(
            {{
                dbt_utils.generate_surrogate_key([
                    'src.src_system', 'src.client_id', 'src.account_name'
                ])
            }}, '-1'
        ) as sk_dim_financial_profile,
        src.account_name as financial_profile,
        src.is_deleted_flag as delete_flag,
        src.src_system as source_system,
        src.client_id as client,
        src.account_type_id as account_type,
        current_timestamp() as current_audit_datetime,
        src.account_class as class_name,
        src.audit_id as stage_id,
        'BSAK_HIST' as stage_source_table,
        src.src_audit_timestamp as source_audit_datetime

    from source as src

)

select
    sk_dim_financial_profile,
    delete_flag,
    source_system,
    client,
    financial_profile,
    class_name,
    account_type,
    current_audit_datetime,
    stage_id,
    stage_source_table,
    source_audit_datetime

from final
