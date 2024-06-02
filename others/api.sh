curl -X GET "http://localhost:8080/api/v1/dags/big_data_pipeline/dagRuns/manual__2024-06-01T19:28:04.831421+00:00" \
-H "Content-Type: application/json" \
-u "admin:admin"

curl -X POST "http://localhost:8080/api/v1/dags/big_data_pipeline/dagRuns" \
-H "Content-Type: application/json" \
-u "admin:admin" \
-d '{
       "conf": {
         "run_task_group": "data_import_group"
       }
}'
