py rdl.py csv://.\integration_tests\csv_source\full_refresh_data\ postgresql+psycopg2://postgres:password@/dest_dw .\integration_tests\csv_source\config\ --log-level INFO --full-refresh yes

psql -U postgres -d dest_dw -a -f .\integration_tests\csv_source\assertions\column_test_full_refresh_assertions.sql






