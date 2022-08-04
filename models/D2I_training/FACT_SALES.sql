{{
    config(
        materialized='incremental',
		
		merge_update_columns = ['campaignid', 'customerid','ORDERDATEID','ORDERLOCATIONID','PRODUCTID','SHIPDATEID']
    )
}}


SELECT
    FACTSALES_SK.nextval AS FACTSALESID
    ,sal.order_id as ORDERID
    ,cust.customerid AS CUSTOMERID
    , camp.campaignid AS CAMPAIGNID
    , ddorder.dateid AS ORDERDATEID
    --, dl.locationid AS ORDERLOCATIONID
    , dp.productid AS PRODUCTID
    , ddship.dateid AS SHIPDATEID
    , SAL.SHIP_MODE AS SHIPMODE
    , SAL.DISCOUNT AS DISCOUNT
    , SAL.PROFIT AS PROFIT
    , SAL.QUANTITY AS QUANTITY
    , SAL.SALES AS SALEAMOUNT
    --, SAL.VALUE_SHIPPING_COST AS SHIPPINGCOST
    ,  current_timestamp() AS CREATEDDATE
    , 'fivetran' AS  CREATEDBY
    , SAL._FIVETRAN_SYNCED AS MODIFIEDDATE
    , 'fivetran' AS  MODIFIEDBY
FROM
    DATA_TO_INSIGHTS.SQL_SERVER_SYNC_DBO.SALESCOMPLETE SAL
    LEFT OUTER JOIN DBT_KT.DIM_CUSTOMER cust on cust.CUSTOMERSOURCEKEY=sal.CUSTOMER_ID
    LEFT OUTER JOIN DBT_KT.DIM_CAMPAIGN camp on camp.CAMPAIGNSOURCEKEY=sal.VALUE_CAMPAIGN_ID
    LEFT OUTER JOIN DATA_TO_INSIGHTS.DATA_TO_INSIGHTS.DIMDATE ddorder on ddorder.date=to_date(sal.ORDER_DATE)
    LEFT OUTER JOIN DATA_TO_INSIGHTS.DATA_TO_INSIGHTS.DIMDATE ddship on ddship.date=to_date(sal.SHIP_DATE)
    LEFT OUTER JOIN DATA_TO_INSIGHTS.DBT_KT.DIM_PRODUCT dp on dp.productsourcekey=sal.PRODUCT_ID


{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  WHERE SAL._FIVETRAN_SYNCED > (select max(MODIFIEDDATE) from  {{ this }})

{% endif %}
/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null