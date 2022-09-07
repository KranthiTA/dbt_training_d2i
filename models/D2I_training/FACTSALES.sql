{{
    config(
        materialized='incremental',
		merge_update_columns = ['campaignid', 'customerid','ORDERDATEID','ORDERLOCATIONID','PRODUCTID','SHIPDATEID']
    )
}}


SELECT
    FACTSALES_SK.nextval AS FACTSALESID,
     cust.customerid AS CUSTOMERID
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
    D2I_TRAINING.SQL_SERVER_SYNC_DBO.SALESCOMPLETE SAL
    LEFT OUTER JOIN D2I_TRAINING.DATA_TO_INSIGHTS.DIMCUSTOMER cust on cust.CUSTOMERSOURCEKEY=sal.CUSTOMER_ID
    LEFT OUTER JOIN D2I_TRAINING.DATA_TO_INSIGHTS.DIMCAMPAIGN camp on camp.CAMPAIGNSOURCEKEY=sal.VALUE_CAMPAIGN_ID
    LEFT OUTER JOIN D2I_TRAINING.DATA_TO_INSIGHTS.DIMDATE ddorder on ddorder.date=to_date(sal.ORDER_DATE)
    LEFT OUTER JOIN D2I_TRAINING.DATA_TO_INSIGHTS.DIMDATE ddship on ddship.date=to_date(sal.SHIP_DATE)
    LEFT OUTER JOIN D2I_TRAINING.DATA_TO_INSIGHTS.DIMPRODUCT dp on dp.productsourcekey=sal.PRODUCT_ID

{% if is_incremental() %}

  ---this filter will only be applied on an incremental run
  WHERE SAL._FIVETRAN_SYNCED > (select max(MODIFIEDDATE) from  {{ this }})

{% endif %}
/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null