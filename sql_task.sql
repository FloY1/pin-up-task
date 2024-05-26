SELECT facility_name
FROM los_angeles_restaurant_health_inspections
WHERE pe_description = (
    SELECT pe_description
    FROM los_angeles_restaurant_health_inspections
    WHERE facility_name LIKE '%CAFE%'
      OR facility_name LIKE '%TEA%'
      OR facility_name LIKE '%JUICE%'
    GROUP BY pe_description
    ORDER BY COUNT(*) DESC
    LIMIT 1 OFFSET 2
);