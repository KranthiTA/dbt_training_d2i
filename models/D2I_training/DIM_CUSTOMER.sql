--customersourcekey

{% set customersourcekey = run_query('select CUSTOMER_ATTRIBUTE_NAME from "DATA_TO_INSIGHTS"."GOOGLE_DRIVE"."MAPPING_TEMPLATE" WHERE D_2_I_ATTRIBUTE_NAME = \'CUSTOMERSOURCEKEY\' AND D_2_I_ENTITY_NAME = \'Customer\'') %}
{% if execute %}
{% set results_list = customersourcekey.columns[0].values() %}
{% else %}
{% set results_list = [] %}
{% endif %}

--FIRSTNAME

{% set firstname = run_query('select CUSTOMER_ATTRIBUTE_NAME from "DATA_TO_INSIGHTS"."GOOGLE_DRIVE"."MAPPING_TEMPLATE" WHERE D_2_I_ATTRIBUTE_NAME = \'FIRSTNAME\' AND D_2_I_ENTITY_NAME = \'Customer\'') %}
{% if execute %}
{% set firstname_list = firstname.columns[0].values() %}
{% else %}
{% set firstname_list = [] %}
{% endif %}

--LASTNAME

{% set lastname = run_query('select CUSTOMER_ATTRIBUTE_NAME from "DATA_TO_INSIGHTS"."GOOGLE_DRIVE"."MAPPING_TEMPLATE" WHERE D_2_I_ATTRIBUTE_NAME = \'LASTNAME\' AND D_2_I_ENTITY_NAME = \'Customer\'') %}
{% if execute %}
{% set lastname_list = lastname.columns[0].values() %}
{% else %}
{% set lastname_list = [] %}
{% endif %}

--AGE

{% set age = run_query('select CUSTOMER_ATTRIBUTE_NAME from "DATA_TO_INSIGHTS"."GOOGLE_DRIVE"."MAPPING_TEMPLATE" WHERE D_2_I_ATTRIBUTE_NAME = \'AGE\' AND D_2_I_ENTITY_NAME = \'Customer\'') %}
{% if execute %}
{% set age_list = age.columns[0].values() %}
{% else %}
{% set age_list = [] %}
{% endif %}

--GENDER

{% set gender = run_query('select CUSTOMER_ATTRIBUTE_NAME from "DATA_TO_INSIGHTS"."GOOGLE_DRIVE"."MAPPING_TEMPLATE" WHERE D_2_I_ATTRIBUTE_NAME = \'GENDER\' AND D_2_I_ENTITY_NAME = \'Customer\'') %}
{% if execute %}
{% set gender_list = gender.columns[0].values() %}
{% else %}
{% set gender_list = [] %}
{% endif %}

--EMAIL

{% set email = run_query('select CUSTOMER_ATTRIBUTE_NAME from "DATA_TO_INSIGHTS"."GOOGLE_DRIVE"."MAPPING_TEMPLATE" WHERE D_2_I_ATTRIBUTE_NAME = \'EMAIL\' AND D_2_I_ENTITY_NAME = \'Customer\'') %}
{% if execute %}
{% set email_list = email.columns[0].values() %}
{% else %}
{% set email_list = [] %}
{% endif %}

--ADDRESS

{% set address = run_query('select CUSTOMER_ATTRIBUTE_NAME from "DATA_TO_INSIGHTS"."GOOGLE_DRIVE"."MAPPING_TEMPLATE" WHERE D_2_I_ATTRIBUTE_NAME = \'ADDRESS\' AND D_2_I_ENTITY_NAME = \'Customer\'') %}
{% if execute %}
{% set address_list = address.columns[0].values() %}
{% else %}
{% set address_list = [] %}
{% endif %}

--ISACTIVE

{% set isactive = run_query('select CUSTOMER_ATTRIBUTE_NAME from "DATA_TO_INSIGHTS"."GOOGLE_DRIVE"."MAPPING_TEMPLATE" WHERE D_2_I_ATTRIBUTE_NAME = \'ISACTIVE\' AND D_2_I_ENTITY_NAME = \'Customer\'') %}
{% if execute %}
{% set isactive_list = isactive.columns[0].values() %}
{% else %}
{% set isactive_list = [] %}
{% endif %}

--TABLE NAME

{% set cust_tablename = run_query('select Top 1 CUSTOMER_ENTITY_TABLE_NAME from "DATA_TO_INSIGHTS"."GOOGLE_DRIVE"."MAPPING_TEMPLATE" WHERE D_2_I_ATTRIBUTE_NAME = \'CUSTOMERSOURCEKEY\' AND D_2_I_ENTITY_NAME = \'Customer\'') %}
{% if execute %}
{% set cust_tablename_list = cust_tablename.columns[0].values() %}
{% else %}
{% set cust_tablename_list = [] %}
{% endif %}


{{
    config(
        materialized='incremental',
		unique_key='CUSTOMERSOURCEKEY',
		incremental_strategy='merge'
    )
}}

    SELECT DimCustomer_SK.nextval as CUSTOMERID,
    {% for customersourcekey in results_list %}
	 
    {{customersourcekey}} AS CUSTOMERSOURCEKEY 
    {% endfor %}
	{% for firstname in firstname_list %}
	{% for lastname in lastname_list %}
		, {{firstname}} ||', '||{{lastname}} AS CUSTOMERNAME 
	 {% endfor %}
	 {% endfor %}
	 {% for age in age_list %}
		, {{age}} AS AGE 
	 {% endfor %}
	 {% for gender in gender_list %}
		, {{gender}}   GENDER 
	 {% endfor %}
	 {% for email in email_list %}
		, {{email}}  EMAIL 
	 {% endfor %}
	 {% for address in address_list %}
		, {{address}} AS ADDRESS
     {% endfor %}
	 {% for isactive in isactive_list %}
		, {{isactive}} AS ISACTIVE
	 {% endfor %}
	, current_timestamp() AS CREATEDDATE 
	, 'fivetran' AS  CREATEDBY 
    , _FIVETRAN_SYNCED AS MODIFIEDDATE
    , 'fivetran' AS  MODIFIEDBY
FROM 
    {% for cust_tablename in cust_tablename_list %}
    DATA_TO_INSIGHTS.DATA_TO_INSIGHTS_RAW.{{cust_tablename}}
	{% endfor %}
  

{% if is_incremental() %}

 -- this filter will only be applied on an incremental run
  WHERE _FIVETRAN_SYNCED > (select max(MODIFIEDDATE) from  {{ this }})

{% endif %}