SELECT
    e.CCSID,
    e.SITELINKREFERENCEID,
    e.TEXT_TEMPLATESITELINKID,
    v.VENUE_NAME,
    e.LASTUPDATEDATE
FROM {{ source('ecomm_application', 'pricing_analytics_events') }} e
LEFT JOIN {{ ref('dim_venue') }} v ON e.VENUE_ID = v.VENUE_ID
