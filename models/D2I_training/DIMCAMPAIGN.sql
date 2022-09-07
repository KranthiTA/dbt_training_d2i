--CAMPAIGNSOURCEKEY

{% set campaignsourcekey = run_query('select CUSTOMER_ATTRIBUTE_NAME from "D2I_TRAINING"."D2I_DATASET"."MAPPING_TEMPLATE" WHERE D_2_I_ATTRIBUTE_NAME =\'CAMPAIGN_ID\' AND D_2_I_ENTITY_NAME = \'Campaign\'') %}
{% if execute %}
{% set campaignsourcekey_list = campaignsourcekey.columns[0].values() %}
{% else %}
{% set campaignsourcekey_list = [] %}
{% endif %}

--CAMPAIGNNAME

{% set campaignname = run_query('select CUSTOMER_ATTRIBUTE_NAME from "D2I_TRAINING"."D2I_DATASET"."MAPPING_TEMPLATE" WHERE D_2_I_ATTRIBUTE_NAME =\'CAMPAIGNNAME\' AND D_2_I_ENTITY_NAME = \'Campaign\'') %}
{% if execute %}
{% set campaignname_list = campaignname.columns[0].values() %}
{% else %}
{% set campaignname_list = [] %}
{% endif %}

--description

{% set description = run_query('select CUSTOMER_ATTRIBUTE_NAME from "D2I_TRAINING"."D2I_DATASET"."MAPPING_TEMPLATE" WHERE D_2_I_ATTRIBUTE_NAME =\'DESCRIPTION\' AND D_2_I_ENTITY_NAME = \'Campaign\'') %}
{% if execute %}
{% set description_list = description.columns[0].values() %}
{% else %}
{% set description_list = [] %}
{% endif %}

--productid

{% set productid = run_query('select CUSTOMER_ATTRIBUTE_NAME from "D2I_TRAINING"."D2I_DATASET"."MAPPING_TEMPLATE" WHERE D_2_I_ATTRIBUTE_NAME =\'PRODUCTID\' AND D_2_I_ENTITY_NAME = \'Campaign\'') %}
{% if execute %}
{% set productid_list = productid.columns[0].values() %}
{% else %}
{% set productid_list = [] %}
{% endif %}


--channel

{% set channel = run_query('select CUSTOMER_ATTRIBUTE_NAME from "D2I_TRAINING"."D2I_DATASET"."MAPPING_TEMPLATE" WHERE D_2_I_ATTRIBUTE_NAME =\'CHANNEL\' AND D_2_I_ENTITY_NAME = \'Campaign\'') %}
{% if execute %}
{% set channel_list = channel.columns[0].values() %}
{% else %}
{% set channel_list = [] %}
{% endif %}

--startdate

{% set startdate = run_query('select CUSTOMER_ATTRIBUTE_NAME from "D2I_TRAINING"."D2I_DATASET"."MAPPING_TEMPLATE" WHERE D_2_I_ATTRIBUTE_NAME =\'CAMPAIGN_DATE\' AND D_2_I_ENTITY_NAME = \'Campaign\'') %}
{% if execute %}
{% set startdate_list = startdate.columns[0].values() %}
{% else %}
{% set startdate_list = [] %}
{% endif %}

--enddate

{% set enddate = run_query('select CUSTOMER_ATTRIBUTE_NAME from "D2I_TRAINING"."D2I_DATASET"."MAPPING_TEMPLATE" WHERE D_2_I_ATTRIBUTE_NAME =\'ENDDATE\' AND D_2_I_ENTITY_NAME = \'Campaign\'') %}
{% if execute %}
{% set enddate_list = enddate.columns[0].values() %}
{% else %}
{% set enddate_list = [] %}
{% endif %}

--isactive

{% set isactive = run_query('select CUSTOMER_ATTRIBUTE_NAME from "D2I_TRAINING"."D2I_DATASET"."MAPPING_TEMPLATE" WHERE D_2_I_ATTRIBUTE_NAME =\'IS_SUCCESS\' AND D_2_I_ENTITY_NAME = \'Campaign\'') %}
{% if execute %}
{% set isactive_list = isactive.columns[0].values() %}
{% else %}
{% set isactive_list = [] %}
{% endif %}

--cost

{% set cost = run_query('select CUSTOMER_ATTRIBUTE_NAME from "D2I_TRAINING"."D2I_DATASET"."MAPPING_TEMPLATE" WHERE D_2_I_ATTRIBUTE_NAME =\'COST\' AND D_2_I_ENTITY_NAME = \'Campaign\'') %}
{% if execute %}
{% set cost_list = cost.columns[0].values() %}
{% else %}
{% set cost_list = [] %}
{% endif %}

--tablename

{% set tablename = run_query('select CUSTOMER_ENTITY_TABLE_NAME from "D2I_TRAINING"."D2I_DATASET"."MAPPING_TEMPLATE" WHERE D_2_I_ATTRIBUTE_NAME = \'COST\' AND D_2_I_ENTITY_NAME = \'Campaign\'') %}
{% if execute %}
{% set tablename_list = tablename.columns[0].values() %}
{% else %}
{% set tablename_list = [] %}
{% endif %}

 {{
    config(
        materialized='incremental',
		unique_key='campaignsourcekey'
    )
}}


SELECT 

    {% for campaignsourcekey in campaignsourcekey_list %}
{{campaignsourcekey}} AS CAMPAIGNID,
    {{campaignsourcekey}} AS CampaignSourceKey 
    {% endfor %}
    {% for campaignname in campaignname_list %}
	,{{campaignname}} as campaignname
	    {% endfor %}
	    {% for description in description_list %}
	,{{description}} as description
	    {% endfor %}
	    {% for productid in productid_list %}
	,{{productid}} as productid
	    {% endfor %}
	    {% for channel in channel_list %}
	,{{channel}} as channel
	    {% endfor %}
	    {% for startdate in startdate_list %}
	,to_date({{startdate}}) as startdate
	    {% endfor %}
	    {% for enddate in enddate_list %}
	,to_date({{enddate}}) as enddate
	    {% endfor %}
	    {% for isactive in isactive_list %}
	,{{isactive}} as isactive
	    {% endfor %}
	    {% for cost in cost_list %}
	,{{cost}} as cost
	    {% endfor %}
	,_FIVETRAN_SYNCED as modifieddate
	,'fivetran' as modifiedby
FROM 
{% for tablename in tablename_list %}
    D2I_TRAINING.D2I_DATASET.{{tablename}}
 {% endfor %}

% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  WHERE _FIVETRAN_SYNCED > (select max(MODIFIEDDATE) from  {{ this }})

{% endif %}

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null