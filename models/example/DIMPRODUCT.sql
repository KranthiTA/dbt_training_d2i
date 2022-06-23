{{
    config(
        materialized='incremental',
		unique_key='ProductSourceKey',
		incremental_strategy='merge'
    )
}}

select 
seq2.nextval as Product_id,
Product_id as PRODUCTSOURCEKEY,
Category as CATEGORY,
Sub_Category as SUBCATEGORY,
product_name as PRODUCTNAME,
 Current_timestamp() as CREATEDDATE	,
'fivetran' as CREATEDBY	,
_FIVETRAN_SYNCED MODIFIEDDATE	,
'fivetran' MODIFIEDBY
    from google_drive.product_complete
 


{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  WHERE _FIVETRAN_SYNCED > (select max(MODIFIEDDATE) from  {{ this }})

{% endif %}
/*
    Uncomment the line below to remove records with null `id` values
*/
