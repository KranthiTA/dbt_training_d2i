{{
    config(
        materialized='incremental',
		
		merge_update_columns = ['campaignid', 'customerid','ORDERDATEID','ORDERLOCATIONID','PRODUCTID','SHIPDATEID']
    )
}}


SELECT
    FACTSALES_SK.nextval AS FACTSALESID,
     cust.customerid AS CUSTOMERID
    --, camp.campaignid AS CAMPAIGNID
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
    , sysdate AS CREATEDDATE
    , 'fivetran' AS  CREATEDBY
    , SAL._FIVETRAN_SYNCED AS MODIFIEDDATE
    , 'fivetran' AS  MODIFIEDBY
FROM
    DATA_TO_INSIGHTS.GOOGLE_DRIVE.SALES_COMPLETE SAL
    LEFT OUTER JOIN DATA_TO_INSIGHTS.GOOGLE_DRIVE.CUSTOMER_COMPLETE cust on cust.CUSTOMERSOURCEKEY=sal.CUSTOMER_ID
    --LEFT OUTER JOIN DATA_TO_INSIGHTS.GOOGLE_DRIVE.STG_CAMPAIGN camp on camp.CAMPAIGNSOURCEKEY=sal.VALUE_CAMPAIGN_ID
    LEFT OUTER JOIN DATA_TO_INSIGHTS.DATA_TO_INSIGHTS.DIMDATE ddorder on ddorder.date=to_date(sal.ORDER_DATE,'mm/dd/yyyy')
    LEFT OUTER JOIN DATA_TO_INSIGHTS.DATA_TO_INSIGHTS.DIMDATE ddship on ddship.date=to_date(sal.SHIP_DATE,'mm/dd/yyyy')
--    LEFT OUTER JOIN DATA_TO_INSIGHTS.DATA_TO_INSIGHTS.DIMLOCATION dl ON dl.region=sal.VALUE_REGION and dl.state=sal.VALUE_STATE
--    and dl.city=sal.VALUE_CITY and dl.country=sal.VALUE_COUNTRY
    LEFT OUTER JOIN DATA_TO_INSIGHTS.GOOGLE_DRIVE.PRODUCT_COMPLETE dp on dp.productsourcekey=sal.PRODUCT_ID
        --and dp.productname=sal.VALUE_PRODUCT_NAME and dp.SEGMENT=sal.VALUE_SEGMENT and dp.subcategory=sal.VALUE_SUB_CATEGORY and --dp.category=sal.VALUE_CATEGORY


{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  WHERE SAL._FIVETRAN_SYNCED > (select max(MODIFIEDDATE) from  {{ this }})

{% endif %}
/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null