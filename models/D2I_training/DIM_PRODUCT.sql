{{
    config(
        materialized='incremental',
		unique_key='ProductSourceKey',
		incremental_strategy='merge'
    )
}}


{% set ProductSourceKey = run_query(' select CUSTOMER_ATTRIBUTE_NAME from "DATA_TO_INSIGHTS"."D2I_DATASET"."MAPPING_TEMPLATE" WHERE D_2_I_ATTRIBUTE_NAME= \'PRODUCT_ID\' AND D_2_I_ENTITY_NAME = \'Product\'')%}
{% if execute %}
{% set results_list = ProductSourceKey.columns[0].values() %}
{% else %}
{% set results_list = [] %}
{% endif %}

{% set productname = run_query('select CUSTOMER_ATTRIBUTE_NAME from "DATA_TO_INSIGHTS"."D2I_DATASET"."MAPPING_TEMPLATE" WHERE D_2_I_ATTRIBUTE_NAME= \'PRODUCT_NAME\' AND D_2_I_ENTITY_NAME = \'Product\'') %}
{% if execute %}
{% set productname_list = productname.columns[0].values() %}
{% else %}
{% set productname_list = [] %}
{% endif %}

{% set segment = run_query('select CUSTOMER_ATTRIBUTE_NAME from "DATA_TO_INSIGHTS"."D2I_DATASET"."MAPPING_TEMPLATE" WHERE D_2_I_ATTRIBUTE_NAME= \'SEGMENT\' AND D_2_I_ENTITY_NAME = \'Product\'') %}
{% if execute %}
{% set segment_list = segment.columns[0].values() %}
{% else %}
{% set segment_list = [] %}
{% endif %}

{% set subcategory = run_query('select CUSTOMER_ATTRIBUTE_NAME from "DATA_TO_INSIGHTS"."D2I_DATASET"."MAPPING_TEMPLATE" WHERE D_2_I_ATTRIBUTE_NAME= \'SUB_CATEGORY\' AND D_2_I_ENTITY_NAME = \'Product\'') %}
{% if execute %}
{% set subcategory_list = subcategory.columns[0].values() %}
{% else %}
{% set subcategory_list = [] %}
{% endif %}

{% set category = run_query('select CUSTOMER_ATTRIBUTE_NAME from "DATA_TO_INSIGHTS"."D2I_DATASET"."MAPPING_TEMPLATE" WHERE D_2_I_ATTRIBUTE_NAME= \'CATEGORY\' AND D_2_I_ENTITY_NAME = \'Product\'') %}
{% if execute %}
{% set category_list = category.columns[0].values() %}
{% else %}
{% set category_list = [] %}
{% endif %}

--tablename

{% set tablename = run_query('select CUSTOMER_ENTITY_TABLE_NAME from "DATA_TO_INSIGHTS"."D2I_DATASET"."MAPPING_TEMPLATE" WHERE D_2_I_ATTRIBUTE_NAME = \'PRODUCT_ID\' AND D_2_I_ENTITY_NAME = \'Product\'') %}
{% if execute %}
{% set tablename_list = tablename.columns[0].values() %}
{% else %}
{% set tablename_list = [] %}
{% endif %}

SELECT 
{% for ProductSourceKey in results_list %}
    ROW_NUMBER() OVER (ORDER BY {{ProductSourceKey}}) AS PRODUCTID,
    {{ProductSourceKey}} AS PRODUCTSOURCEKEY
{% endfor %}
{% for productname in productname_list %},
{{productname}} as PRODUCTNAME
{% endfor %}
{% for segment in segment_list %},
{{segment}} as SEGMENT
{% endfor %}
{% for subcategory in subcategory_list %},
{{subcategory}} as SUBCATEGORY
{% endfor %}
{% for category in category_list %},{{category}} AS CATEGORY
{% endfor %},
current_timestamp() AS CREATEDDATE,
'fivetran' AS  CREATEDBY,
_FIVETRAN_SYNCED AS MODIFIEDDATE,
'fivetran' AS  MODIFIEDBY
FROM 
{% for tablename in tablename_list %}
    DATA_TO_INSIGHTS.D2I_DATASET.{{tablename}}
{% endfor %}

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  WHERE _FIVETRAN_SYNCED > (select max(MODIFIEDDATE) from  {{ this }})

{% endif %}