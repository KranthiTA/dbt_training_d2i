{{
    config(
        materialized='incremental',
		unique_key='customersourcekey',
		incremental_strategy='merge'
    )
}}

select 
seq1.nextval as CUSTOMERID
,CUSTOMER_ID as CUSTOMERSOURCEKEY
,CUSTOMER_NAME as CUSTOMERNAME
,null as AGE
,null as GENDER
,null as EMAIL
,concat(CITY,',',"STATE",',',COUNTRY,',',REGION,',',POSTAL_CODE) as "ADDRESS"
,null as ISACTIVE
,Current_timestamp() as CREATEDDATE	
,'fivetran' as CREATEDBY	
,_FIVETRAN_SYNCED MODIFIEDDATE	
,'fivetran' MODIFIEDBY
from DATA_TO_INSIGHTS.GOOGLE_DRIVE.CUSTOMER_COMPLETE



{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  WHERE _FIVETRAN_SYNCED > (select max(MODIFIEDDATE) from  {{ this }})

{% endif %}
/*
    Uncomment the line below to remove records with null `id` values
*/
