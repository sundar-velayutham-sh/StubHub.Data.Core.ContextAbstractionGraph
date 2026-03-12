SELECT
    CCSID,
    SITELINKREFERENCEID,
    TEXT_TEMPLATESITELINKID,
    PCID,
    LASTUPDATEDATE
FROM {{ source('ecomm_application', 'pricing_analytics_events') }}
