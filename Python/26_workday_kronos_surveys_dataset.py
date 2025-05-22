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
    """Generate realistic employee survey data with templated responses"""
    
    # Templates for survey responses by satisfaction level
    positive_templates = [
        "I'm very satisfied with my role. The {team} is {supportive} and provides excellent opportunities.",
        "The {workplace} culture is {great}. Management really {listens} to our feedback.",
        "I feel {valued} in my position. The work-life balance is {excellent}.",
        "Our department has {great} leadership and clear communication channels.",
        "{Excellent} benefits and compensation package. I feel well rewarded for my work."
    ]
    
    neutral_templates = [
        "The work environment is {okay}. Some processes could be more efficient.",
        "{Average} satisfaction with my current role and responsibilities.",
        "Management is {decent}, but communication could be improved.",
        "Work-life balance is {acceptable}, though there's room for improvement.",
        "The team dynamics are {fine}, but we could use more collaboration."
    ]
    
    negative_templates = [
        "Feeling {overworked} and {stressed} due to high workload.",
        "Limited growth opportunities and {poor} communication from management.",
        "Work-life balance is {terrible}. Too many overtime hours required.",
        "{Inadequate} support and resources to perform effectively.",
        "Company culture needs significant improvement. Morale is {low}."
    ]

    # Word variations for more natural responses
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
        "low": ["low", "poor", "declining", "concerning"]
    }

    def generate_response(score):
        """Generate a response based on satisfaction score"""
        if score >= 4:
            template = random.choice(positive_templates)
        elif score >= 3:
            template = random.choice(neutral_templates)
        else:
            template = random.choice(negative_templates)
            
        # Replace template placeholders with variations
        response = template
        for key, options in variations.items():
            if "{" + key + "}" in response:
                response = response.replace("{" + key + "}", random.choice(options))
        return response

    # Generate survey data
    data = {
        'survey_id': range(1, n + 1),
        'employee_id': [random.choice(employee_ids) for _ in range(n)],
        'survey_date': [fake.date_between(start_date='-1y', end_date='today') for _ in range(n)],
        'satisfaction_score': [random.randint(1, 5) for _ in range(n)]
    }
    
    # Add responses based on satisfaction scores
    data['response'] = [generate_response(score) for score in data['satisfaction_score']]
    
    df = pd.DataFrame(data)
    df.to_csv('survey_data.csv', index=False)
    return df

# Generate 2000 records
survey_df = generate_survey_data(2000)
print("\nMuestra de encuestas generadas:")
print("\nID\tScore\tRespuesta")
print("-" * 80)
for _, row in survey_df.head().iterrows():
    print(f"{row['survey_id']}\t{row['satisfaction_score']}\t{row['response'][:100]}...")