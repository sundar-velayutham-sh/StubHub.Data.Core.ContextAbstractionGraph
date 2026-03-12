SELECT
    CCSID,
    SITELINKREFERENCEID,
    TEXT_TEMPLATESITELINKID,
    ROUND(PRICE * 1.1, 2) AS PRICE_WITH_MARKUP,
    LASTUPDATEDATE
FROM {{ source('ecomm_application', 'pricing_analytics_events') }}
