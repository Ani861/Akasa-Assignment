1. Clone the Repository
git clone https://github.com/Ani861/Akasa-Assignment
cd Akasa-Assignment

2. Create and Activate Virtual Environment
On Windows:
      python -m venv venv
      venv\Scripts\activate

On macOS / Linux:
     python3 -m venv venv
     source venv/bin/activate

3. Install Dependencies
   pip install -r requirements.txt

4. Prepare Input Data

Ensure your data files are placed correctly:

       sampledata/
            ├── Task_DE_new_customers.csv
            └── Task_DE_new_orders.xml

5. Set Up MySQL Database

Open MySQL and create a database:

   CREATE DATABASE etl_db;
   USE etl_db;


Run the schema file:

mysql -u root -p etl_db < schema.sql

6. Run the ETL Script

Choose your environment:

🔹 Local (Pandas)
python etl.py

🔹 Databricks / Spark

Upload and run:

data.py

🔹 With MySQL Integration
python data.py


7. Verify Output

Check:

MySQL tables (customers, orders, dead_letter, etc.)

KPI results printed in the console .
