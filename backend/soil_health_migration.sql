ALTER TABLE agrorisk_db.farmer_submissions
    ADD COLUMN soil_ph DECIMAL(5, 2) NOT NULL DEFAULT 7.20,
    ADD COLUMN nitrogen_n DECIMAL(10, 2) NOT NULL DEFAULT 150.00,
    ADD COLUMN phosphorus_p DECIMAL(10, 2) NOT NULL DEFAULT 25.00,
    ADD COLUMN potassium_k DECIMAL(10, 2) NOT NULL DEFAULT 180.00,
    ADD COLUMN fertilizer_recommendation LONGTEXT NULL;
