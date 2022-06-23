{{
    config(
        materialized='incremental'
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
	        from {{source('DIM_PRODUCT','PRODUCT_COMPLETE')}}
{% if is_incremental() %}  


{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  WHERE _FIVETRAN_SYNCED > (select max(MODIFIEDDATE) from  {{ this }})

{% endif %}
/*
    Uncomment the line below to remove records with null `id` values
*/
