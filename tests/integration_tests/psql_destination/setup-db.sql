/* IMPORTANT:  this script must be run on the target database within the target server */

-- create extensions
CREATE EXTENSION IF NOT EXISTS CITEXT;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- create user
CREATE USER rdl_integration_test_user WITH ENCRYPTED PASSWORD 'rdl_integration_test_password';

-- setup user
GRANT CONNECT ON DATABASE rdl_integration_test_target_db TO rdl_integration_test_user;
GRANT CREATE ON DATABASE rdl_integration_test_target_db TO rdl_integration_test_user;
