# Apache Airflow Integration Guide

This guide explains how to use Apache Airflow to orchestrate your E-commerce Big Data Medallion Architecture pipeline.

## What Was Added

### 1. Airflow Folder Structure
```
airflow/
├── dags/                    # DAG files
│   └── ecommerce_pipeline_dag.py
├── logs/                    # Airflow logs
└── plugins/                 # Custom Airflow plugins
```

### 2. Docker Compose Services
Added to `docker-compose.yml`:
- **postgres**: PostgreSQL database for Airflow metadata
- **airflow-init**: Initializes Airflow database and creates admin user
- **airflow-webserver**: Airflow web UI (port 8082)
- **airflow-scheduler**: Airflow scheduler for executing DAGs

### 3. DAG File
Created `airflow/dags/ecommerce_pipeline_dag.py` with the following pipeline:
```
scraper → sync_to_minio → bronze_to_silver → silver_to_gold → load_to_mysql
```

## How to Start Airflow

### Step 1: Start All Services
```powershell
docker-compose up -d
```

This will start:
- Zookeeper, Kafka, Kafka UI, Kafka init
- MySQL, phpMyAdmin
- MinIO, MinIO init
- PostgreSQL, Airflow init, Airflow webserver, Airflow scheduler

### Step 2: Verify Airflow is Running
```powershell
docker ps
```

You should see:
- `ecommerce_postgres`
- `ecommerce_airflow_init` (exits after initialization)
- `ecommerce_airflow_webserver`
- `ecommerce_airflow_scheduler`

### Step 3: Check Airflow Logs (if needed)
```powershell
docker logs ecommerce_airflow_webserver
docker logs ecommerce_airflow_scheduler
```

## Access Airflow UI

### URL
```
http://localhost:8082
```

### Default Login Credentials
- **Username**: `admin`
- **Password**: `admin`

⚠️ **IMPORTANT**: Change these credentials in production by modifying the `airflow-init` service in `docker-compose.yml`.

## How to Use Airflow UI

### 1. View Your DAG
1. Open http://localhost:8082
2. Login with `admin` / `admin`
3. Click on **DAGs** in the left sidebar
4. Find `ecommerce_pipeline_dag`

### 2. Trigger DAG Manually
1. Click on the DAG name `ecommerce_pipeline_dag`
2. Click the **Play** button (▶) in the top right corner
3. Select **Trigger DAG**
4. The DAG will start executing

### 3. Monitor Task Execution
1. Click on the DAG name
2. Click on **Graph View** to see task dependencies
3. Click on **Task Instances** to see task status
4. Click on individual tasks to view logs

### 4. View Task Logs
1. Click on a task instance
2. Click on **Log** to see the execution logs
3. You can see the output of your Python scripts here

## DAG Configuration

### Schedule
The DAG is configured to run **every day at midnight** (`@daily`).

To change the schedule, edit `airflow/dags/ecommerce_pipeline_dag.py`:
```python
schedule_interval='@daily',  # Change this
```

Common schedules:
- `@daily` - Every day at midnight
- `@hourly` - Every hour
- `@weekly` - Every week
- `@monthly` - Every month
- `0 9 * * *` - Every day at 9 AM (cron format)

### Retries
Tasks will retry **2 times** with a **5-minute delay** between retries.

To change retry settings, edit the `default_args` in the DAG:
```python
'retries': 2,
'retry_delay': timedelta(minutes=5),
```

### Email Notifications
Emails are sent on task failure to `admin@example.com`.

To change email settings, edit the `default_args`:
```python
'email': ['your-email@example.com'],
'email_on_failure': True,
```

## Docker Commands

### Start All Services
```powershell
docker-compose up -d
```

### Stop All Services
```powershell
docker-compose down
```

### Restart Airflow Services Only
```powershell
docker-compose restart airflow-webserver airflow-scheduler
```

### View Airflow Webserver Logs
```powershell
docker logs ecommerce_airflow_webserver -f
```

### View Airflow Scheduler Logs
```powershell
docker logs ecommerce_airflow_scheduler -f
```

### Reinitialize Airflow Database
```powershell
docker-compose down
docker volume rm ecommerce-bigdata_postgres_data
docker-compose up -d postgres airflow-init
```

## Troubleshooting

### DAG Not Appearing
1. Check if the DAG file exists: `airflow/dags/ecommerce_pipeline_dag.py`
2. Check Airflow webserver logs: `docker logs ecommerce_airflow_webserver`
3. Restart Airflow webserver: `docker-compose restart airflow-webserver`

### Tasks Failing
1. Click on the failed task in the Airflow UI
2. View the logs to see the error
3. Check if all dependencies (Kafka, MySQL, MinIO) are running
4. Verify the script paths in the DAG are correct

### Port Conflicts
If port 8082 is already in use, change it in `docker-compose.yml`:
```yaml
airflow-webserver:
  ports:
    - "8083:8080"  # Change to 8083 or another port
```

Then update the environment variable:
```yaml
AIRFLOW__WEBSERVER__BASE_URL: http://localhost:8083
```

### Airflow Database Issues
If you encounter database errors:
```powershell
docker-compose down
docker volume rm ecommerce-bigdata_postgres_data
docker-compose up -d postgres airflow-init
docker-compose up -d airflow-webserver airflow-scheduler
```

## Architecture Overview

### Pipeline Flow
```
scraper.py
    ↓ (produces to Kafka books-raw)
sync_to_minio.py
    ↓ (syncs CSV to MinIO)
bronze_to_silver.py
    ↓ (transforms bronze to silver)
silver_to_gold.py
    ↓ (enriches silver to gold)
load_to_mysql.py
    ↓ (loads gold to MySQL)
```

### Airflow Task Dependencies
```
scraper_task → sync_to_minio_task → bronze_to_silver_task → silver_to_gold_task → load_to_mysql_task
```

Each task uses `BashOperator` to execute the corresponding Python script.

## Security Notes

### Change Default Credentials
Edit `docker-compose.yml` in the `airflow-init` service:
```yaml
airflow users create \
  --username your_username \
  --firstname Your \
  --lastname Name \
  --role Admin \
  --email your-email@example.com \
  --password your_secure_password
```

### Change Secret Key
Edit the environment variables in `docker-compose.yml`:
```yaml
AIRFLOW__WEBSERVER__SECRET_KEY: 'your_secure_random_key_here'
```

Generate a secure key:
```powershell
python -c "import secrets; print(secrets.token_hex(32))"
```

## Next Steps

1. **Test the DAG**: Trigger it manually from the Airflow UI
2. **Monitor Execution**: Watch the task logs to ensure everything runs smoothly
3. **Adjust Schedule**: Change the schedule interval to match your needs
4. **Add More DAGs**: Create additional DAGs for other workflows
5. **Set Up Monitoring**: Configure email notifications for production

## Useful Links

- Airflow Documentation: https://airflow.apache.org/docs/
- DAG Authoring Guide: https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html
- Operators Reference: https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/operators.html
