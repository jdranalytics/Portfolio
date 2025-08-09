import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta

# Initialize Faker for realistic data
fake = Faker()

# Load headcount data
workday_df = pd.read_csv('https://raw.githubusercontent.com/ringoquimico/Portfolio/refs/heads/main/Data%20Sources/workday_data.csv')

# Define retail-specific courses
courses = [
    {'name': 'Customer Service Excellence', 'mandatory': True, 'duration_hours': 4, 'relevant_depts': ['Sales', 'Marketing']},
    {'name': 'Inventory Management Basics', 'mandatory': True, 'duration_hours': 3, 'relevant_depts': ['Inventory']},
    {'name': 'Workplace Safety and Compliance', 'mandatory': True, 'duration_hours': 2, 'relevant_depts': ['Sales', 'Inventory', 'HR', 'IT', 'Marketing', 'Finance']},
    {'name': 'Retail Sales Techniques', 'mandatory': False, 'duration_hours': 5, 'relevant_depts': ['Sales']},
    {'name': 'Advanced Inventory Control', 'mandatory': False, 'duration_hours': 4, 'relevant_depts': ['Inventory']},
    {'name': 'Data Security Awareness', 'mandatory': True, 'duration_hours': 2, 'relevant_depts': ['IT', 'Finance']},
    {'name': 'Leadership and Team Management', 'mandatory': False, 'duration_hours': 6, 'relevant_depts': ['HR', 'Sales', 'Inventory', 'Marketing', 'Finance'], 'relevant_roles': ['Manager', 'Supervisor']},
    {'name': 'Financial Reporting for Retail', 'mandatory': False, 'duration_hours': 4, 'relevant_depts': ['Finance']},
    {'name': 'Marketing Campaign Strategies', 'mandatory': False, 'duration_hours': 5, 'relevant_depts': ['Marketing']},
    {'name': 'Employee Wellness Program', 'mandatory': False, 'duration_hours': 2, 'relevant_depts': ['HR']}
]

def generate_training_data(workday_df, target_records=20000):
    """Generate synthetic training data for 2024 and 2025 plans."""
    data = {
        'training_id': [],
        'employee_id': [],
        'course_name': [],
        'status': [],
        'training_date': [],
        'plan_year': [],
        'duration_hours': []
    }
    
    # Filter active and on-leave employees
    eligible_employees = workday_df[workday_df['status'].isin(['Active', 'On Leave'])]['employee_id'].tolist()
    
    # Estimate records per employee (average 10 courses per year × 2 years)
    records_per_employee = target_records // len(eligible_employees)
    if records_per_employee < 16:  # Minimum 8 courses per year × 2 years
        records_per_employee = 16
    elif records_per_employee > 24:  # Maximum 12 courses per year × 2 years
        records_per_employee = 24
    
    for employee_id in eligible_employees:
        employee = workday_df[workday_df['employee_id'] == employee_id].iloc[0]
        dept = employee['department']
        role = employee['job_role']
        
        # Select relevant courses for the employee
        relevant_courses = [
            c for c in courses 
            if c['mandatory'] or dept in c['relevant_depts']
        ]
        relevant_courses = [
            c for c in relevant_courses 
            if 'relevant_roles' not in c or role in c.get('relevant_roles', [])
        ]
        
        # Assign 8-12 courses per year
        num_courses_2024 = random.randint(8, 12)
        num_courses_2025 = random.randint(8, 12)
        
        for year in [2024, 2025]:
            num_courses = num_courses_2024 if year == 2024 else num_courses_2025
            assigned_courses = random.sample(relevant_courses, min(len(relevant_courses), num_courses))
            
            for course in assigned_courses:
                training_id = len(data['training_id']) + 1
                data['training_id'].append(training_id)
                data['employee_id'].append(employee_id)
                data['course_name'].append(course['name'])
                data['duration_hours'].append(course['duration_hours'])
                data['plan_year'].append(year)
                
                # Assign status and dates
                if year == 2024:
                    if random.random() < 0.7:  # 70% chance of completion in 2024
                        status = 'Completed'
                        training_date = fake.date_between(start_date=datetime(2024, 1, 1), end_date=datetime(2024, 12, 31))
                    else:
                        status = 'Scheduled'
                        training_date = fake.date_between(start_date=datetime(2024, 6, 1), end_date=datetime(2025, 3, 31))
                else:  # 2025 plan
                    if random.random() < 0.5 and fake.date_between(start_date=datetime(2025, 1, 1), end_date=datetime(2025, 5, 23)) <= datetime(2025, 5, 23).date():
                        status = 'Completed'
                        training_date = fake.date_between(start_date=datetime(2025, 1, 1), end_date=datetime(2025, 5, 23))
                    else:
                        status = 'Scheduled'
                        training_date = fake.date_between(start_date=datetime(2025, 5, 24), end_date=datetime(2025, 12, 31))
                
                data['status'].append(status)
                data['training_date'].append(training_date)
    
    # Trim or pad to exactly 20000 records
    training_df = pd.DataFrame(data)
    if len(training_df) > target_records:
        training_df = training_df.sample(n=target_records, random_state=42)
    elif len(training_df) < target_records:
        # Add more records if needed
        additional_records = target_records - len(training_df)
        for _ in range(additional_records):
            employee_id = random.choice(eligible_employees)
            employee = workday_df[workday_df['employee_id'] == employee_id].iloc[0]
            dept = employee['department']
            role = employee['job_role']
            
            relevant_courses = [
                c for c in courses 
                if c['mandatory'] or dept in c['relevant_depts']
            ]
            relevant_courses = [
                c for c in relevant_courses 
                if 'relevant_roles' not in c or role in c.get('relevant_roles', [])
            ]
            course = random.choice(relevant_courses)
            year = random.choice([2024, 2025])
            
            training_id = len(data['training_id']) + 1
            data['training_id'].append(training_id)
            data['employee_id'].append(employee_id)
            data['course_name'].append(course['name'])
            data['duration_hours'].append(course['duration_hours'])
            data['plan_year'].append(year)
            
            if year == 2024:
                if random.random() < 0.7:
                    status = 'Completed'
                    training_date = fake.date_between(start_date=datetime(2024, 1, 1), end_date=datetime(2024, 12, 31))
                else:
                    status = 'Scheduled'
                    training_date = fake.date_between(start_date=datetime(2024, 6, 1), end_date=datetime(2025, 3, 31))
                data['status'].append(status)
                data['training_date'].append(training_date)
            else:
                if random.random() < 0.5 and fake.date_between(start_date=datetime(2025, 1, 1), end_date=datetime(2025, 5, 23)) <= datetime(2025, 5, 23).date():
                    status = 'Completed'
                    training_date = fake.date_between(start_date=datetime(2025, 1, 1), end_date=datetime(2025, 5, 23))
                else:
                    status = 'Scheduled'
                    training_date = fake.date_between(start_date=datetime(2025, 5, 24), end_date=datetime(2025, 12, 31))
                data['status'].append(status)
                data['training_date'].append(training_date)
    
    # Create final DataFrame
    training_df = pd.DataFrame(data)
    
    # Calculate completion percentage for each employee and year
    completion_data = []
    for employee_id in eligible_employees:
        for year in [2024, 2025]:
            employee_trainings = training_df[(training_df['employee_id'] == employee_id) & (training_df['plan_year'] == year)]
            total_courses = len(employee_trainings)
            completed_courses = len(employee_trainings[employee_trainings['status'] == 'Completed'])
            completion_percentage = (completed_courses / total_courses * 100) if total_courses > 0 else 0
            completion_data.append({
                'employee_id': employee_id,
                'plan_year': year,
                'total_courses': total_courses,
                'completed_courses': completed_courses,
                'completion_percentage': round(completion_percentage, 2)
            })
    
    completion_df = pd.DataFrame(completion_data)
    
    # Save to CSV
    training_df.to_csv('training_data.csv', index=False)
    completion_df.to_csv('training_completion.csv', index=False)
    
    return training_df, completion_df

# Generate training data
training_df, completion_df = generate_training_data(workday_df, 20000)

# Print sample data
print("Sample Training Data:")
print(training_df.head())
print("\nSample Completion Data:")
print(completion_df.head())