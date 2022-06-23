{{
    config(
        materialized='incremental',
		unique_key='ProductSourceKey',
		incremental_strategy='merge'
    )
}}

select 
seq2.nextval as PRODUCTID
,Product_id as PRODUCTSOURCEKEY
,product_name as PRODUCTNAME
,null as SEGMENT
,Sub_Category as SUBCATEGORY
,Category as CATEGORY
,Current_timestamp() as CREATEDDATE
,'fivetran' as CREATEDBY
,_FIVETRAN_SYNCED as MODIFIEDDATE
,'fivetran' as MODIFIEDBY
from DATA_TO_INSIGHTS.GOOGLE_DRIVE.PRODUCT_COMPLETE


/*
{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  WHERE _FIVETRAN_SYNCED > (select max(MODIFIEDDATE) from  {{ this }})

{% endif %}*/
/*
    Uncomment the line below to remove records with null `id` values
*/
