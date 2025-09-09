-- Deployment script for Cortex Analyst Analytics Streamlit App
-- Run these commands in Snowflake to set up the app

-- 1. Create necessary databases and schemas
CREATE DATABASE IF NOT EXISTS SNOWFLAKE_INTELLIGENCE;
CREATE SCHEMA IF NOT EXISTS SNOWFLAKE_INTELLIGENCE.PUBLIC;

-- 2. Create a stage for the Streamlit app files
CREATE OR REPLACE STAGE SNOWFLAKE_INTELLIGENCE.PUBLIC.CORTEX_ANALYTICS_STAGE;

-- 3. Upload the application files to the stage
-- You'll need to upload these files manually through Snowsight or using PUT commands:
-- - cortex_analyst_analytics_app.py (or rename to streamlit_app.py)
-- - environment.yml

-- Example PUT commands (run these from SnowSQL or programmatically):
/*
PUT file://path/to/cortex_analyst_analytics_app.py @SNOWFLAKE_INTELLIGENCE.PUBLIC.CORTEX_ANALYTICS_STAGE;
PUT file://path/to/environment.yml @SNOWFLAKE_INTELLIGENCE.PUBLIC.CORTEX_ANALYTICS_STAGE;
*/

-- 4. Create the Streamlit app
CREATE OR REPLACE STREAMLIT SNOWFLAKE_INTELLIGENCE.PUBLIC.CORTEX_ANALYST_ANALYTICS
ROOT_LOCATION = '@SNOWFLAKE_INTELLIGENCE.PUBLIC.CORTEX_ANALYTICS_STAGE'
MAIN_FILE = 'cortex_analyst_analytics_app.py'  -- Change to 'streamlit_app.py' if renamed
QUERY_WAREHOUSE = 'COMPUTE_WH';  -- Replace with your warehouse name

-- 5. Grant necessary permissions
-- Replace YOUR_ROLE with the appropriate role
GRANT USAGE ON DATABASE SNOWFLAKE_INTELLIGENCE TO ROLE YOUR_ROLE;
GRANT USAGE ON SCHEMA SNOWFLAKE_INTELLIGENCE.PUBLIC TO ROLE YOUR_ROLE;
GRANT USAGE ON STREAMLIT SNOWFLAKE_INTELLIGENCE.PUBLIC.CORTEX_ANALYST_ANALYTICS TO ROLE YOUR_ROLE;

-- Grant access to account usage (required for the analytics)
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE YOUR_ROLE;

-- Grant access to Cortex Analyst functions
GRANT USAGE ON SCHEMA SNOWFLAKE.LOCAL TO ROLE YOUR_ROLE;

-- 6. Show the Streamlit app URL
SHOW STREAMLITS IN SCHEMA SNOWFLAKE_INTELLIGENCE.PUBLIC;

-- The app will be available at a URL like:
-- https://app.snowflake.com/REGION/ACCOUNT/w/WORKSHEET_ID#/streamlit-apps/SNOWFLAKE_INTELLIGENCE.PUBLIC.CORTEX_ANALYST_ANALYTICS
