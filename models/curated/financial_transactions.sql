{{
    config(
        tags=["SC_VW"]
    )
}}

/*  CTE to fetch data from source object fct_financial_transactions */
with fct_financial_transactions as (

    select
        current_audit_datetime,
        source_system,
        transaction_id,
        account_id,
        transaction_date,
        amount_local,
        currency_code,
        transaction_type,
        approval_status,
        audit_id,
        source_audit_datetime,
        sk_fact_financial_transaction
    from {{ ref("fct_financial_transactions") }}
),

/*  CTE to create a model from fct_financial_transactions as per ADS document */
final as (

    select
        greatest(a.current_audit_datetime) as dp_timestamp,
        a.source_system as source_system,
        a.transaction_id as transaction_id,
        a.account_id as account_id,
        a.transaction_date as transaction_date,
        a.amount_local as amount_local,
        a.currency_code as currency_code,
        a.transaction_type as transaction_type,
        a.approval_status as approval_status,
        a.audit_id as audit_id,
        a.source_audit_datetime as source_audit_datetime
    from fct_financial_transactions as a
    where a.sk_fact_financial_transaction <> '-1'

)

select
    dp_timestamp,
    source_system,
    transaction_id,
    account_id,
    transaction_date,
    amount_local,
    currency_code,
    transaction_type,
    approval_status,
    audit_id,
    source_audit_datetime

from final
