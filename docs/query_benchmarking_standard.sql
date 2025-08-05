--Script to get various benchmark standards in a table that can be joined to the base adult 4 data
WITH A4_BASE ("Area_Code", "Area_Name", "Area_Type", JOIN_KEY, "Cancer_Site", "Gender", "Years_Since_Diagnosis", "Date_Diagnosis_Window", "Survival_Per") AS (
    SELECT 
        "Area_Code", "Area_Name", "Area_Type",
        CONCAT("Cancer_Site", "Gender", "Years_Since_Diagnosis", "Date_Diagnosis_Window") AS JOIN_KEY, 
        "Cancer_Site", "Gender", "Years_Since_Diagnosis", "Date_Diagnosis_Window", "Survival_Per"
    FROM DATA_LAB_NCL_TRAINING_TEMP.CANCER__SURVIVAL.ADULT_4
    WHERE 
        "Standardisation_Type" = 'Age-standardised' 
        AND "Survival_Metric" = 'Net Survival'
)

SELECT 
    JOIN_KEY,
    "'England'" AS ENGLAND,
    "'London'" AS LONDON,
    "'Best'" AS BEST,
    "'Worst'" AS WORST,
    "'Q1'" AS Q1,
    "'Q2'" AS Q2,
    "'Q3'" AS Q3
FROM (     
    --Set up Benchmark Standards
    ----England
    SELECT 
        JOIN_KEY,
        "Area_Name" AS "Standard",
        "Survival_Per"
    FROM A4_BASE
    WHERE "Area_Code" = 'E92000001'
    
    UNION ALL
    ----London
    SELECT 
        JOIN_KEY,
        "Area_Name" AS "Standard",
        "Survival_Per"
    FROM A4_BASE
    WHERE "Area_Code" = 'E40000003'
    
    UNION ALL
    ----Best
    SELECT 
        JOIN_KEY,
        'Best' AS "Standard",
        MAX("Survival_Per")
    FROM A4_BASE
    WHERE "Area_Type" = 'Cancer Alliance'
    GROUP BY JOIN_KEY
    
    UNION ALL
    ----Worst
    SELECT 
        JOIN_KEY,
        'Worst' AS "Standard",
        MIN("Survival_Per")
    FROM A4_BASE
    WHERE "Area_Type" = 'Cancer Alliance'
    GROUP BY JOIN_KEY
    
    UNION ALL
    ----Q1
    SELECT 
        JOIN_KEY,
        'Q1' AS "Standard",
        PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY "Survival_Per") AS percentile_value
    FROM A4_BASE
    WHERE "Area_Type" = 'Cancer Alliance'
    GROUP BY JOIN_KEY
    
    UNION ALL
    ----Q2
    SELECT 
        JOIN_KEY,
        'Q2' AS "Standard",
        PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY "Survival_Per") AS percentile_value
    FROM A4_BASE
    WHERE "Area_Type" = 'Cancer Alliance'
    GROUP BY JOIN_KEY
    
    UNION ALL
    ----Q3
    SELECT 
        JOIN_KEY,
        'Q3' AS "Standard",
        PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY "Survival_Per") AS percentile_value
    FROM A4_BASE
    WHERE "Area_Type" = 'Cancer Alliance'
    GROUP BY JOIN_KEY
) std
PIVOT (
    SUM("Survival_Per") 
    FOR "Standard" IN ('England', 'London', 'Best', 'Worst', 'Q1', 'Q2', 'Q3')
)
ORDER BY JOIN_KEY