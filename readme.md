
## How to Run the Project

  

### Prerequisites

Make sure the following are installed on your system:

- Docker Desktop
- Git
- Any web browser to access Airflow and PostgreSQL.

  

---

  

### 1. Clone the Repository

```bash
git  clone <repository-url>
cd <project-root>
```

### 2. Start the Environment

```bash
cd  infra
docker compose up -d
```

  

### 3. Access Airflow

Open a browser and navigate to:
```bash
http:localhost:8999 # Airflow

# Login Credentials:
username = admin
password = admin
```
Once logged in, unpause all DAGs and run **"dag_master_shopzada_pipeline.py"**


### 4. Access PostgreSQL (Optional)
You can access PostgreSQL in order to check the staging, dimensions, and fact tables.
```bash
http:localhost:5050 # PostgreSQL

# Login Credentials
username = admin@admin.com
password = admin

# After logging in, right click "Servers" under "Object Explorer" in the left-hand panel.
# Left click "Register a Server"
# Server Credentials
General Tab;
- Name = shopzada
Connection Tab:
- Hostname 				= postgres
- Port 					= 5432
- Maintenance database 	= airflow
- Username 				= airflow
- Password 				= airflow
```
