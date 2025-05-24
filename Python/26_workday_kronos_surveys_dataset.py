import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta, date

# Initialize Faker for realistic data
fake = Faker()

# Define realistic gender-specific first names
male_names = [fake.first_name_male() for _ in range(500)]
female_names = [fake.first_name_female() for _ in range(500)]

# Role-based age distribution for realism
role_age_ranges = {
    'Manager': (35, 60),
    'Supervisor': (30, 55),
    'Analyst': (25, 45),
    'Associate': (22, 40),
    'Clerk': (18, 35)
}

# Department-based overtime multipliers for June and December
overtime_multipliers = {
    'Sales': {'June': 1.5, 'December': 2.0, 'default': 1.0},
    'Inventory': {'June': 1.2, 'December': 1.5, 'default': 1.0},
    'HR': {'default': 0.8},
    'IT': {'default': 0.9},
    'Marketing': {'June': 1.3, 'December': 1.3, 'default': 1.0},
    'Finance': {'default': 0.7}
}

def generate_workday_data(n=1000):
    data = {
        'employee_id': range(1001, 1001 + n),
        'first_name': [],
        'last_name': [fake.last_name() for _ in range(n)],
        'gender': [],
        'age': [],
        'department': [random.choice(['Sales', 'Inventory', 'HR', 'IT', 'Marketing', 'Finance']) for _ in range(n)],
        'job_role': [],
        'hire_date': [fake.date_between(start_date='-10y', end_date='-30d') for _ in range(n)],
        'termination_date': [None] * n,
        'onleave_date': [None] * n,
        'salary': [],
        'location': [random.choice(['New York', 'Los Angeles', 'Chicago', 'Houston', 'Miami']) for _ in range(n)],
        'status': [],
        'performance_score': []
    }

    for i in range(n):
        # Gender and name assignment
        gender = random.choice(['Male', 'Female'])
        data['gender'].append(gender)
        data['first_name'].append(random.choice(male_names if gender == 'Male' else female_names))

        # Role assignment with department consistency
        role = random.choice(['Manager', 'Associate', 'Analyst', 'Clerk', 'Supervisor'])
        data['job_role'].append(role)

        # Age based on role
        age_range = role_age_ranges[role]
        age = random.randint(age_range[0], age_range[1])
        data['age'].append(age)

        # Salary based on role and age
        base_salary = {
            'Manager': random.uniform(80000, 120000),
            'Supervisor': random.uniform(60000, 90000),
            'Analyst': random.uniform(50000, 80000),
            'Associate': random.uniform(40000, 60000),
            'Clerk': random.uniform(30000, 50000)
        }[role]
        data['salary'].append(round(base_salary * (1 + (age - 18) * 0.01), 2))

        # Status logic
        hire_date = data['hire_date'][i]
        hire_date_dt = datetime.combine(hire_date, datetime.min.time())
        days_since_hire = (datetime.today() - hire_date_dt).days

        if random.random() < 0.15:  # 15% chance of termination
            data['status'].append('Terminated')
            max_days = min(365 * 5, days_since_hire)
            if max_days < 30:
                max_days = 30
            days_employed = random.randint(30, max_days)
            data['termination_date'][i] = hire_date + timedelta(days=days_employed)
        elif random.random() < 0.1:  # 10% chance of on leave
            data['status'].append('On Leave')
            max_days = days_since_hire
            if max_days < 30:
                max_days = 30
            days_employed = random.randint(30, max_days)
            data['onleave_date'][i] = hire_date + timedelta(days=days_employed)
        else:
            data['status'].append('Active')

        # Performance score with termination correlation
        if data['status'][i] == 'Terminated' and random.random() < 0.6:
            data['performance_score'].append(random.randint(1, 5))
        else:
            data['performance_score'].append(random.randint(1, 10))

    df = pd.DataFrame(data)
    df['hire_date'] = pd.to_datetime(df['hire_date'])
    df['termination_date'] = pd.to_datetime(df['termination_date'])
    df['onleave_date'] = pd.to_datetime(df['onleave_date'])
    df.to_csv('workday_data.csv', index=False)
    return df

def generate_kronos_data(n=5000, workday_df=None):
    employee_ids = workday_df['employee_id'].tolist()
    data = {
        'entry_id': range(1, n + 1),
        'employee_id': [],
        'work_date': [],
        'hours_worked': [],
        'shift_type': [],
        'overtime_hours': []
    }

    for _ in range(n):
        emp_id = random.choice(employee_ids)
        emp_data = workday_df[workday_df['employee_id'] == emp_id].iloc[0]
        hire_date = emp_data['hire_date']
        termination_date = emp_data['termination_date']
        onleave_date = emp_data['onleave_date']
        department = emp_data['department']

        # Ensure work_date is after hire_date and respects status
        max_date = datetime.today()
        if termination_date is not pd.NaT:
            max_date = min(max_date, termination_date)
        if onleave_date is not pd.NaT:
            max_date = min(max_date, onleave_date)

        if max_date <= hire_date:
            continue

        work_date = fake.date_between(start_date=hire_date, end_date=max_date)
        data['employee_id'].append(emp_id)
        data['work_date'].append(work_date)

        # Simulate absences (5% chance of hours_worked = 0 and overtime = 0 on weekdays)
        is_weekday = work_date.weekday() < 5  # Monday to Friday
        if is_weekday and random.random() < 0.05:  # 5% chance of absence
            data['hours_worked'].append(0.0)
            data['overtime_hours'].append(0.0)
            data['shift_type'].append('None')  # No shift for absences
        else:
            data['hours_worked'].append(round(random.uniform(4, 8), 2))
            data['shift_type'].append(random.choice(['Morning', 'Afternoon', 'Night']))
            # Overtime with seasonal increase for June and December
            month = work_date.month
            multiplier = overtime_multipliers.get(department, {'default': 1.0}).get(
                'June' if month == 6 else 'December' if month == 12 else 'default', 1.0)
            overtime = round(random.uniform(0, 4) * multiplier, 2) if random.random() > 0.8 else 0
            data['overtime_hours'].append(overtime)

    df = pd.DataFrame(data)
    df['work_date'] = pd.to_datetime(df['work_date'])
    df.to_csv('kronos_data.csv', index=False)
    return df

def generate_survey_data(n=2000, workday_df=None):
    employee_ids = workday_df['employee_id'].tolist()
    
    # Adjusted score distribution for 70% positive, 20% neutral, 10% negative
    score_distribution = [5] * int(n * 0.35) + [4] * int(n * 0.35) + [3] * int(n * 0.20) + [1, 2] * int(n * 0.05)
    random.shuffle(score_distribution)

    positive_templates = [
        "I'm very satisfied with my role. The {team} is {supportive} and provides {excellent} opportunities.",
        "The {workplace} culture is {great}. Management really {listens} to our feedback.",
        "I feel {valued} in my position. The work-life balance is {excellent}.",
        "Our department has {great} leadership and clear communication channels.",
        "{excellent} benefits and compensation package. I feel well rewarded for my work."
    ]
    
    neutral_templates = [
        "The work environment is {okay}. Some processes could be more efficient.",
        "{average} satisfaction with my current role and responsibilities.",
        "Management is {decent}, but communication could be improved.",
        "Work-life balance is {acceptable}, though there's room for improvement.",
        "The team dynamics are {fine}, but we could use more collaboration."
    ]
    
    negative_templates = [
        "Feeling {overworked} and {stressed} due to high workload.",
        "Limited growth opportunities and {poor} communication from management.",
        "Work-life balance is {terrible}. Too many overtime hours required.",
        "{inadequate} support and resources to perform effectively.",
        "Company culture needs significant improvement. Morale is {low}."
    ]

    variations = {
        "team": ["team", "department", "group", "unit"],
        "supportive": ["supportive", "helpful", "encouraging", "collaborative"],
        "workplace": ["workplace", "work environment", "office", "company"],
        "great": ["great", "excellent", "fantastic", "outstanding"],
        "listens": ["listens", "responds", "acts on", "considers"],
        "valued": ["valued", "appreciated", "respected", "recognized"],
        "excellent": ["excellent", "exceptional", "superior", "wonderful"],
        "okay": ["okay", "adequate", "reasonable", "satisfactory"],
        "decent": ["decent", "fair", "moderate", "acceptable"],
        "fine": ["fine", "alright", "average", "standard"],
        "overworked": ["overworked", "overwhelmed", "exhausted", "burned out"],
        "stressed": ["stressed", "pressured", "tense", "anxious"],
        "poor": ["poor", "inadequate", "insufficient", "lacking"],
        "terrible": ["terrible", "awful", "poor", "unacceptable"],
        "low": ["low", "poor", "declining", "concerning"],
        "average": ["average", "moderate", "typical", "standard"],
        "acceptable": ["acceptable", "tolerable", "satisfactory", "manageable"],
        "inadequate": ["inadequate", "insufficient", "poor", "lacking"]
    }

    def generate_response(score):
        if score >= 4:
            template = random.choice(positive_templates)
        elif score == 3:
            template = random.choice(neutral_templates)
        else:
            template = random.choice(negative_templates)
        response = template
        for key, options in variations.items():
            placeholder = "{" + key + "}"
            if placeholder in response:
                response = response.replace(placeholder, random.choice(options))
        return response

    data = {
        'survey_id': range(1, n + 1),
        'employee_id': [random.choice(employee_ids) for _ in range(n)],
        'survey_date': [],
        'satisfaction_score': score_distribution[:n],
        'response': []
    }

    for i in range(n):
        emp_id = data['employee_id'][i]
        emp_data = workday_df[workday_df['employee_id'] == emp_id].iloc[0]
        hire_date = emp_data['hire_date']
        termination_date = emp_data['termination_date']
        max_date = termination_date if termination_date is not pd.NaT else datetime.today()
        survey_date = fake.date_between(start_date=hire_date, end_date=max_date)
        data['survey_date'].append(survey_date)
        data['response'].append(generate_response(data['satisfaction_score'][i]))

    df = pd.DataFrame(data)
    df['survey_date'] = pd.to_datetime(df['survey_date'])
    df.to_csv('survey_data.csv', index=False)
    return df

def calculate_absenteeism(kronos_df):
    # Identify weekdays (Monday to Friday, weekday() < 5)
    kronos_df['is_weekday'] = kronos_df['work_date'].apply(lambda x: x.weekday() < 5)
    
    # Count total weekday records
    total_weekday_records = kronos_df[kronos_df['is_weekday']].shape[0]
    
    # Count absences (hours_worked == 0 and overtime_hours == 0 on weekdays)
    absences = kronos_df[(kronos_df['is_weekday']) & 
                         (kronos_df['hours_worked'] == 0) & 
                         (kronos_df['overtime_hours'] == 0)].shape[0]
    
    # Calculate absenteeism percentage
    if total_weekday_records > 0:
        absenteeism_percentage = (absences / total_weekday_records) * 100
    else:
        absenteeism_percentage = 0.0
    
    return {
        'total_weekday_records': total_weekday_records,
        'absences': absences,
        'absenteeism_percentage': round(absenteeism_percentage, 2)
    }

# Generate datasets
workday_df = generate_workday_data(1000)
kronos_df = generate_kronos_data(5000, workday_df)
survey_df = generate_survey_data(2000, workday_df)

# Calculate absenteeism
absenteeism_result = calculate_absenteeism(kronos_df)

# Print samples
print("\nWorkday Data Sample:")
print(workday_df.head())
print("\nKronos Data Sample:")
print(kronos_df.head())
print("\nSurvey Data Sample:")
print("\nID\tScore\tResponse")
print("-" * 80)
for _, row in survey_df.head().iterrows():
    print(f"{row['survey_id']}\t{row['satisfaction_score']}\t{row['response'][:100]}...")
print("\nAbsenteeism Statistics:")
print(f"Total Weekday Records: {absenteeism_result['total_weekday_records']}")
print(f"Absences: {absenteeism_result['absences']}")
print(f"Absenteeism Percentage: {absenteeism_result['absenteeism_percentage']}%")