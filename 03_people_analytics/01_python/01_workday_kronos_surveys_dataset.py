import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta

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

# U.S. federal holidays 2024-2025 (simplified list)
us_holidays = [
    datetime(2024, 1, 1),  # New Year's Day
    datetime(2024, 1, 15),  # Martin Luther King Jr. Day
    datetime(2024, 2, 19),  # Presidents' Day
    datetime(2024, 5, 27),  # Memorial Day
    datetime(2024, 6, 19),  # Juneteenth
    datetime(2024, 7, 4),   # Independence Day
    datetime(2024, 9, 2),   # Labor Day
    datetime(2024, 11, 11), # Veterans Day
    datetime(2024, 11, 28), # Thanksgiving
    datetime(2024, 12, 25), # Christmas
    datetime(2025, 1, 1),   # New Year's Day
    datetime(2025, 1, 20),  # Martin Luther King Jr. Day
    datetime(2025, 2, 17),  # Presidents' Day
    datetime(2025, 5, 26),  # Memorial Day
]

def is_workday(date):
    return date.weekday() < 5 and date not in us_holidays

def generate_workday_data(n=1000):
    # Define shift distribution: 40% Morning, 40% Afternoon, 20% Night
    shift_types = ['Morning'] * 400 + ['Afternoon'] * 400 + ['Night'] * 200
    random.shuffle(shift_types)

    data = {
        'employee_id': range(1001, 1001 + n),
        'first_name': [],
        'last_name': [fake.last_name() for _ in range(n)],
        'gender': [],
        'age': [],
        'department': [random.choice(['Sales', 'Inventory', 'HR', 'IT', 'Marketing', 'Finance']) for _ in range(n)],
        'job_role': [],
        'shift_type': shift_types,
        'hire_date': [fake.date_between(start_date='-10y', end_date='-30d') for _ in range(n)],
        'termination_date': [None] * n,
        'onleave_date': [None] * n,
        'salary': [],
        'location': [random.choice(['New York', 'Los Angeles', 'Chicago', 'Houston', 'Miami']) for _ in range(n)],
        'status': [],
        'performance_score': []
    }

    for i in range(n):
        gender = random.choice(['Male', 'Female'])
        data['gender'].append(gender)
        data['first_name'].append(random.choice(male_names if gender == 'Male' else female_names))
        role = random.choice(['Manager', 'Associate', 'Analyst', 'Clerk', 'Supervisor'])
        data['job_role'].append(role)
        age_range = role_age_ranges[role]
        age = random.randint(age_range[0], age_range[1])
        data['age'].append(age)
        base_salary = {
            'Manager': random.uniform(80000, 120000),
            'Supervisor': random.uniform(60000, 90000),
            'Analyst': random.uniform(50000, 80000),
            'Associate': random.uniform(40000, 60000),
            'Clerk': random.uniform(30000, 50000)
        }[role]
        data['salary'].append(round(base_salary * (1 + (age - 18) * 0.01), 2))
        hire_date = data['hire_date'][i]
        hire_date_dt = datetime.combine(hire_date, datetime.min.time())
        days_since_hire = (datetime.today() - hire_date_dt).days

        if random.random() < 0.15:
            data['status'].append('Terminated')
            max_days = min(365 * 5, days_since_hire)
            if max_days < 30:
                max_days = 30
            days_employed = random.randint(30, max_days)
            data['termination_date'][i] = hire_date + timedelta(days=days_employed)
        elif random.random() < 0.1:
            data['status'].append('On Leave')
            max_days = days_since_hire
            if max_days < 30:
                max_days = 30
            days_employed = random.randint(30, max_days)
            data['onleave_date'][i] = hire_date + timedelta(days=days_employed)
        else:
            data['status'].append('Active')
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

def generate_kronos_data(workday_df=None):
    employee_ids = workday_df['employee_id'].tolist()
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2025, 5, 23, 23, 59)  # Last workday before May 24 (Saturday)
    data = {
        'entry_id': [],
        'employee_id': [],
        'work_date': [],
        'hours_worked': [],
        'shift_type': [],
        'overtime_hours': []
    }

    entry_count = 0
    for emp_id in employee_ids:
        emp_data = workday_df[workday_df['employee_id'] == emp_id].iloc[0]
        hire_date = emp_data['hire_date']
        termination_date = emp_data['termination_date']
        onleave_date = emp_data['onleave_date']
        department = emp_data['department']
        employee_shift = emp_data['shift_type']

        current_date = start_date
        while current_date <= end_date:
            if current_date < hire_date:
                current_date += timedelta(days=1)
                continue
            if termination_date is not pd.NaT and current_date > termination_date:
                break
            if onleave_date is not pd.NaT and current_date > onleave_date:
                current_date += timedelta(days=1)
                continue
            if is_workday(current_date):
                data['entry_id'].append(entry_count + 1)
                data['employee_id'].append(emp_id)
                data['work_date'].append(current_date)
                if random.random() < 0.05:  # 5% chance of absence
                    data['hours_worked'].append(0.0)
                    data['overtime_hours'].append(0.0)
                    data['shift_type'].append('None')
                else:
                    data['hours_worked'].append(round(random.uniform(4, 8), 2))
                    data['shift_type'].append(employee_shift)
                    month = current_date.month
                    multiplier = overtime_multipliers.get(department, {'default': 1.0}).get(
                        'June' if month == 6 else 'December' if month == 12 else 'default', 1.0)
                    overtime = round(random.uniform(0, 4) * multiplier, 2) if random.random() > 0.8 else 0
                    data['overtime_hours'].append(overtime)
                entry_count += 1
            current_date += timedelta(days=1)

    df = pd.DataFrame(data)
    df['work_date'] = pd.to_datetime(df['work_date'])
    df.to_csv('kronos_data.csv', index=False)
    return df

def generate_survey_data(n=2000, workday_df=None):
    employee_ids = workday_df['employee_id'].tolist()
    score_distribution = [5] * int(n * 0.35) + [4] * int(n * 0.35) + [3] * int(n * 0.20) + [1, 2] * int(n * 0.05)
    random.shuffle(score_distribution)

    # 100 positive templates
    positive_templates = [
        "I'm thrilled with my role. The {team} is {supportive} and offers {excellent} growth.",
        "The {workplace} vibe is {great}. Leadership truly {listens} to us.",
        "I feel {valued} daily. The work-life balance is {excellent}.",
        "Our {team} shows {great} teamwork and clear goals.",
        "The {excellent} pay and perks make me feel appreciated.",
        "Working here is a joy. The {team} is {supportive} and innovative.",
        "The {workplace} environment is {great} for productivity.",
        "I’m proud of how {listens} our management is to ideas.",
        "This job offers {excellent} opportunities for learning.",
        "The {team} culture is {great} and very inclusive.",
        "Feeling {valued} boosts my motivation every day.",
        "The {excellent} leadership guides us effectively.",
        "Our {workplace} is {great} for career development.",
        "I love how {listens} the company is to feedback.",
        "The {excellent} benefits package is a game-changer.",
        "The {team} is {supportive} and fosters collaboration.",
        "Work-life balance here is {excellent} and refreshing.",
        "The {great} management style inspires me.",
        "I enjoy the {excellent} training opportunities provided.",
        "The {workplace} is {great} for professional growth.",
        "Feeling {valued} by peers and leaders is amazing.",
        "The {excellent} support system helps me thrive.",
        "Our {team} has {great} communication and trust.",
        "The {workplace} culture is {excellent} and engaging.",
        "I appreciate how {listens} the team is to concerns.",
        "The {great} work environment boosts my morale.",
        "The {excellent} leadership encourages innovation.",
        "Our {team} is {supportive} and highly motivated.",
        "Work-life balance is {excellent} and well-managed.",
        "The {great} company values align with mine.",
        "I feel {valued} for my contributions daily.",
        "The {excellent} perks make this job special.",
        "The {team} is {great} at solving challenges.",
        "The {workplace} is {excellent} for teamwork.",
        "Management {listens} and acts on our input.",
        "The {great} culture here is very positive.",
        "I love the {excellent} career progression here.",
        "The {team} is {supportive} and encouraging.",
        "Work-life balance is {great} and sustainable.",
        "The {excellent} leadership is very approachable.",
        "Our {workplace} is {great} for new ideas.",
        "Feeling {valued} improves my job satisfaction.",
        "The {excellent} training is top-notch.",
        "The {team} shows {great} dedication daily.",
        "The {workplace} is {excellent} and inspiring.",
        "I appreciate how {listens} the leaders are.",
        "The {great} environment fosters creativity.",
        "The {excellent} support is always available.",
        "Our {team} is {supportive} and collaborative.",
        "Work-life balance is {great} and respected.",
        "The {excellent} company culture is unique.",
        "The {team} is {great} at building trust.",
        "The {workplace} is {excellent} for growth.",
        "Management {listens} and values our opinions.",
        "The {great} leadership motivates us all.",
        "I feel {valued} in every project I join.",
        "The {excellent} benefits are outstanding.",
        "The {team} is {supportive} and dynamic.",
        "Work-life balance is {great} here.",
        "The {excellent} pay reflects my effort.",
        "The {team} shows {great} teamwork skills.",
        "The {workplace} is {excellent} and welcoming.",
        "I love how {listens} the company is.",
        "The {great} culture enhances my day.",
        "The {excellent} leadership is inspiring.",
        "Our {team} is {supportive} and united.",
        "Work-life balance is {great} and balanced.",
        "The {excellent} opportunities are endless.",
        "The {team} is {great} at problem-solving.",
        "The {workplace} is {excellent} for learning.",
        "Management {listens} to every suggestion.",
        "The {great} environment is motivating.",
        "I feel {valued} by the whole team.",
        "The {excellent} support is exceptional.",
        "The {team} shows {great} enthusiasm.",
        "The {workplace} is {excellent} and fun.",
        "The {great} leadership drives success.",
        "Our {team} is {supportive} and strong.",
        "Work-life balance is {excellent} daily.",
        "The {excellent} culture is empowering.",
        "The {team} is {great} and dedicated.",
        "The {workplace} is {excellent} for all.",
        "Management {listens} and supports us.",
        "The {great} environment lifts spirits.",
        "I feel {valued} in this role.",
        "The {excellent} perks are fantastic.",
        "The {team} is {supportive} always.",
        "Work-life balance is {great} and fair."
    ]

    # 100 neutral templates
    neutral_templates = [
        "The work environment is {okay} most days.",
        "I have {average} satisfaction with my role.",
        "Management is {decent}, but could improve.",
        "Work-life balance is {acceptable} here.",
        "The team dynamics are {fine} overall.",
        "The {workplace} is {okay} for now.",
        "My job satisfaction is {average} lately.",
        "Leadership is {decent} but inconsistent.",
        "The balance is {acceptable} some days.",
        "Teamwork is {fine} with some issues.",
        "The {workplace} feels {okay} to me.",
        "I’m {average} happy with my tasks.",
        "Management is {decent} at times.",
        "Work-life balance is {acceptable} mostly.",
        "The {team} is {fine} but quiet.",
        "The environment is {okay} for work.",
        "Satisfaction is {average} right now.",
        "Leadership seems {decent} occasionally.",
        "Balance is {acceptable} with effort.",
        "Team efforts are {fine} usually.",
        "The {workplace} is {okay} to handle.",
        "My role feels {average} today.",
        "Management is {decent} sometimes.",
        "Work-life is {acceptable} on average.",
        "The {team} is {fine} with me.",
        "The place is {okay} for duties.",
        "I’m {average} content with work.",
        "Leadership is {decent} now and then.",
        "Balance feels {acceptable} often.",
        "Teamwork is {fine} with adjustments.",
        "The {workplace} is {okay} enough.",
        "Satisfaction is {average} for me.",
        "Management seems {decent} at points.",
        "Work-life is {acceptable} generally.",
        "The {team} is {fine} to work with.",
        "The area is {okay} for tasks.",
        "I feel {average} about my job.",
        "Leadership is {decent} on occasion.",
        "Balance is {acceptable} with planning.",
        "Team dynamics are {fine} mostly.",
        "The {workplace} is {okay} daily.",
        "My satisfaction is {average} now.",
        "Management is {decent} at moments.",
        "Work-life balance is {acceptable} still.",
        "The {team} feels {fine} today.",
        "The space is {okay} for work.",
        "I’m {average} pleased with this.",
        "Leadership is {decent} sometimes.",
        "Balance is {acceptable} with care.",
        "Teamwork is {fine} with support.",
        "The {workplace} is {okay} always.",
        "Satisfaction is {average} currently.",
        "Management is {decent} at times.",
        "Work-life is {acceptable} often.",
        "The {team} is {fine} enough.",
        "The area feels {okay} today.",
        "I feel {average} about duties.",
        "Leadership is {decent} now.",
        "Balance is {acceptable} usually.",
        "Team efforts are {fine} still.",
        "The {workplace} is {okay} for me.",
        "My role is {average} satisfying.",
        "Management is {decent} occasionally.",
        "Work-life balance is {acceptable} yet.",
        "The {team} is {fine} with me.",
        "The place is {okay} to be.",
        "I’m {average} happy here.",
        "Leadership is {decent} at points.",
        "Balance feels {acceptable} often.",
        "Teamwork is {fine} with effort.",
        "The {workplace} is {okay} still.",
        "Satisfaction is {average} today.",
        "Management seems {decent} sometimes.",
        "Work-life is {acceptable} always.",
        "The {team} is {fine} enough.",
        "The area is {okay} for now.",
        "I feel {average} about work.",
        "Leadership is {decent} on occasion.",
        "Balance is {acceptable} with time.",
        "Team dynamics are {fine} mostly."
    ]

    # 100 negative templates
    negative_templates = [
        "I feel {overworked} with this workload.",
        "The {poor} communication is frustrating.",
        "Work-life balance is {terrible} here.",
        "I’m {stressed} by constant demands.",
        "The {inadequate} support is a problem.",
        "Feeling {overworked} drains my energy.",
        "The {poor} leadership is disappointing.",
        "Balance is {terrible} with overtime.",
        "I’m {stressed} about my role.",
        "The {inadequate} resources hurt productivity.",
        "I feel {overworked} daily now.",
        "The {poor} culture is demotivating.",
        "Work-life is {terrible} to manage.",
        "I’m {stressed} by poor planning.",
        "The {inadequate} training is a setback.",
        "Feeling {overworked} affects my health.",
        "The {poor} management is unhelpful.",
        "Balance is {terrible} always.",
        "I’m {stressed} with no support.",
        "The {inadequate} tools slow me down.",
        "I feel {overworked} constantly.",
        "The {poor} feedback is unconstructive.",
        "Work-life is {terrible} here.",
        "I’m {stressed} by tight deadlines.",
        "The {inadequate} guidance is lacking.",
        "Feeling {overworked} is exhausting.",
        "The {poor} environment is tough.",
        "Balance is {terrible} to maintain.",
        "I’m {stressed} by unclear goals.",
        "The {inadequate} systems fail often.",
        "I feel {overworked} every week.",
        "The {poor} support is unreliable.",
        "Work-life is {terrible} currently.",
        "I’m {stressed} by heavy tasks.",
        "The {inadequate} communication breaks us.",
        "Feeling {overworked} is overwhelming.",
        "The {poor} leadership lacks vision.",
        "Balance is {terrible} with no end.",
        "I’m {stressed} by long hours.",
        "The {inadequate} resources are a burden.",
        "I feel {overworked} too much.",
        "The {poor} culture is toxic.",
        "Work-life is {terrible} always.",
        "I’m {stressed} by poor teamwork.",
        "The {inadequate} tools are outdated.",
        "Feeling {overworked} hurts morale.",
        "The {poor} management is distant.",
        "Balance is {terrible} daily.",
        "I’m {stressed} by no recognition.",
        "The {inadequate} support is minimal.",
        "I feel {overworked} regularly.",
        "The {poor} feedback is ignored.",
        "Work-life is {terrible} still.",
        "I’m {stressed} by high pressure.",
        "The {inadequate} guidance is absent.",
        "Feeling {overworked} is constant.",
        "The {poor} environment is draining.",
        "Balance is {terrible} always.",
        "I’m {stressed} by unclear roles.",
        "The {inadequate} systems are broken.",
        "I feel {overworked} often.",
        "The {poor} support is lacking.",
        "Work-life is {terrible} now.",
        "I’m {stressed} by overload.",
        "The {inadequate} communication fails.",
        "Feeling {overworked} is routine.",
        "The {poor} leadership is weak.",
        "Balance is {terrible} still.",
        "I’m {stressed} by no breaks.",
        "The {inadequate} resources are scarce.",
        "I feel {overworked} daily.",
        "The {poor} culture is negative.",
        "Work-life is {terrible} always.",
        "I’m {stressed} by poor morale.",
        "The {inadequate} tools are useless.",
        "Feeling {overworked} is unbearable.",
        "The {poor} management is detached.",
        "Balance is {terrible} forever.",
        "I’m {stressed} by chaos.",
        "The {inadequate} support is gone.",
        "I feel {overworked} every day.",
        "The {poor} feedback is useless.",
        "Work-life is {terrible} still.",
        "I’m {stressed} by no guidance.",
        "The {inadequate} systems collapse."
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
        max_date = termination_date if termination_date is not pd.NaT else datetime(2025, 5, 23, 23, 59)
        survey_date = fake.date_between(start_date=hire_date, end_date=max_date)
        data['survey_date'].append(survey_date)
        data['response'].append(generate_response(data['satisfaction_score'][i]))
    df = pd.DataFrame(data)
    df['survey_date'] = pd.to_datetime(df['survey_date'])
    df.to_csv('survey_data.csv', index=False)
    return df

def calculate_absenteeism(kronos_df):
    kronos_df['is_weekday'] = kronos_df['work_date'].apply(lambda x: x.weekday() < 5)
    total_weekday_records = kronos_df[kronos_df['is_weekday']].shape[0]
    absences = kronos_df[(kronos_df['is_weekday']) & 
                         (kronos_df['hours_worked'] == 0) & 
                         (kronos_df['overtime_hours'] == 0)].shape[0]
    absenteeism_percentage = (absences / total_weekday_records) * 100 if total_weekday_records > 0 else 0.0
    return {
        'total_weekday_records': total_weekday_records,
        'absences': absences,
        'absenteeism_percentage': round(absenteeism_percentage, 2)
    }

# Generate datasets
workday_df = generate_workday_data(1000)
kronos_df = generate_kronos_data(workday_df)
survey_df = generate_survey_data(2000, workday_df)

# Calculate absenteeism
absenteeism_result = calculate_absenteeism(kronos_df)

# Verify shift distribution in Workday
shift_distribution = workday_df['shift_type'].value_counts()
print("\nDistribución de empleados por turno en Workday:")
print(shift_distribution)

# Verify shift distribution in Kronos (excluding absences)
kronos_shift_distribution = kronos_df[kronos_df['shift_type'] != 'None']['shift_type'].value_counts()
print("\nDistribución de registros por turno en Kronos (excluyendo ausencias):")
print(kronos_shift_distribution)

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