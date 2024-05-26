SELECT facility_name
FROM los_angeles_restaurant_health_inspections
WHERE pe_description = (
    SELECT pe_description
    FROM los_angeles_restaurant_health_inspections
    WHERE facility_name ~* 'CAFE|TEA|JUICE'
    GROUP BY pe_description
    ORDER BY COUNT(*) DESC
    LIMIT 1 OFFSET 2
);