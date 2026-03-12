SELECT
    CCSID,
    SITELINKREFERENCEID,
    TEXT_TEMPLATESITELINKID,
    USERID AS USER_ID,
    LASTUPDATEDATE
FROM {{ source('ecomm_application', 'pricing_analytics_events') }}
