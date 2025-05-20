import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta

# Initialize Faker for realistic data
fake = Faker()

# Generate Workday dataset
def generate_workday_data(n=1000):
    data = {
        'employee_id': range(1001, 1001 + n),
        'first_name': [fake.first_name() for _ in range(n)],
        'last_name': [fake.last_name() for _ in range(n)],
        'department': [random.choice(['Sales', 'Inventory', 'HR', 'IT', 'Marketing', 'Finance']) for _ in range(n)],
        'job_role': [random.choice(['Manager', 'Associate', 'Analyst', 'Clerk', 'Supervisor']) for _ in range(n)],
        'hire_date': [fake.date_between(start_date='-5y', end_date='today') for _ in range(n)],
        'salary': [round(random.uniform(30000, 120000), 2) for _ in range(n)],
        'location': [random.choice(['New York', 'Los Angeles', 'Chicago', 'Houston', 'Miami']) for _ in range(n)],
        'status': [random.choice(['Active', 'Terminated', 'On Leave']) for _ in range(n)]
    }
    df = pd.DataFrame(data)
    df.to_csv('workday_data.csv', index=False)
    return df

# Generate 1000 records
workday_df = generate_workday_data(1000)
print(workday_df.head())

def generate_kronos_data(n=5000, employee_ids=range(1001, 2001)):
    data = {
        'entry_id': range(1, n + 1),
        'employee_id': [random.choice(employee_ids) for _ in range(n)],
        'work_date': [fake.date_between(start_date='-1y', end_date='today') for _ in range(n)],
        'hours_worked': [round(random.uniform(4, 12), 2) for _ in range(n)],
        'shift_type': [random.choice(['Morning', 'Afternoon', 'Night']) for _ in range(n)],
        'overtime_hours': [round(random.uniform(0, 4), 2) if random.random() > 0.8 else 0 for _ in range(n)]
    }
    df = pd.DataFrame(data)
    df.to_csv('kronos_data.csv', index=False)
    return df

# Generate 5000 records
kronos_df = generate_kronos_data(5000)
print(kronos_df.head())

def generate_survey_data(n=2000, employee_ids=range(1001, 2001)):
    data = {
        'survey_id': range(1, n + 1),
        'employee_id': [random.choice(employee_ids) for _ in range(n)],
        'survey_date': [fake.date_between(start_date='-1y', end_date='today') for _ in range(n)],
        'response': [fake.sentence(nb_words=10) for _ in range(n)],
        'satisfaction_score': [random.randint(1, 5) for _ in range(n)]
    }
    df = pd.DataFrame(data)
    df.to_csv('survey_data.csv', index=False)
    return df

# Generate 2000 records
survey_df = generate_survey_data(2000)
print(survey_df.head())