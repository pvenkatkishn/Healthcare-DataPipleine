#Table_Creation
CREATE TABLE healthcare_combined_data (
    location_name VARCHAR(255),
    year_id INT,
    sex_name VARCHAR(10),
    tobacco_value FLOAT,
    alcohol_value FLOAT,
    calcium_intake_g_per_day FLOAT,
    fiber_intake_g_per_day FLOAT,
    fruit_intake_g_per_day FLOAT,
    legumes_intake_g_per_day FLOAT,
    milk_intake_g_per_day FLOAT,
    nuts_intake_g_per_day FLOAT,
    omega3_intake_g_per_day FLOAT,
    processed_meat_intake_g_per_day FLOAT,
    pufa_intake_g_per_day FLOAT,
    red_meat_intake_g_per_day FLOAT,
    sodium_intake_g_per_day FLOAT,
    ssbs_intake_g_per_day FLOAT,
    transfat_intake_g_per_day FLOAT,
    veg_intake_g_per_day FLOAT,
    wholegrains_intake_g_per_day FLOAT
);

#loadind_data
COPY healthcare_combined_data
FROM 's3://s3healthdata/Lifestyledata_combined_v5/'
IAM_ROLE 'arn:aws:iam::637423174652:role/service-role/AmazonRedshift-CommandsAccessRole-20240909T081610'
CSV
IGNOREHEADER 1
DELIMITER ','

#Data_Manipulation
SELECT location_name, year_id, sex_name, 
    ROUND(CAST(tobacco_value AS DECIMAL(10,2)), 2) AS tobacco_value,
    ROUND(CAST(alcohol_value AS DECIMAL(10,2)), 2) AS alcohol_value ,
    ROUND(calcium_intake_g_per_day, 2) as calcium_intake_g_per_day ,
    ROUND(fiber_intake_g_per_day, 2) as fibre_intake_g_per_day,
    ROUND(fruit_intake_g_per_day, 2) as fruit_intake_g_per_day,
    ROUND(legumes_intake_g_per_day, 2) as legumes_intake_g_per_day,
    ROUND(milk_intake_g_per_day, 2) as milk_intake_g_per_day,
    ROUND(nuts_intake_g_per_day, 2) as nuts_intake_g_per_day,
    ROUND(omega3_intake_g_per_day, 2) as omega3_intake_g_per_day,
    ROUND(processed_meat_intake_g_per_day, 2) as processed_meat_intake_g_per_day,
    ROUND(pufa_intake_g_per_day, 2) as pufa_intake_g_per_day,
    ROUND(red_meat_intake_g_per_day, 2) as red_meat_intake_g_per_day,
    ROUND(sodium_intake_g_per_day, 2) as sodium_intake_g_per_day,
    ROUND(ssbs_intake_g_per_day, 2) as ssbs_intake_g_per_day,
    ROUND(transfat_intake_g_per_day, 2) as transfat_intake_g_per_day,
    ROUND(veg_intake_g_per_day, 2) as veg_intake_g_per_day,
    ROUND(wholegrains_intake_g_per_day, 2) as wholegrains_intake_g_per_day
FROM healthcare_combined_data
ORDER BY year_id
