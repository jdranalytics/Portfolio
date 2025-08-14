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
    'Supervisor': (30, 60),
    'Analyst': (25, 45),
    'Associate': (22, 45),
    'Clerk': (18, 45)
}

# Department-based overtime multipliers for June and December
overtime_multipliers = {
    'Sales': {'June': 1.2, 'November': 1.2, 'December': 2.0, 'default': 1.0},
    'Inventory': {'June': 1.2, 'November': 1.2, 'December': 2.0, 'default': 1.0},
    'HR': {'default': 0.8},
    'IT': {'default': 0.9},
    'Marketing': {'June': 1.3, 'November': 1.3, 'December': 1.2, 'default': 1.0},
    'Finance': {'default': 0.7}
}

# Ajusta el rango de fechas global
DATA_START_DATE = datetime(2022, 1, 1)
DATA_END_DATE = datetime(2025, 5, 23)

# U.S. federal holidays 2022-2025 (solo dentro del rango)
us_holidays = [
    # 2022
    datetime(2022, 1, 1),   # New Year's Day
    datetime(2022, 1, 17),  # Martin Luther King Jr. Day
    datetime(2022, 2, 21),  # Presidents' Day
    datetime(2022, 5, 30),  # Memorial Day
    datetime(2022, 6, 19),  # Juneteenth
    datetime(2022, 7, 4),   # Independence Day
    datetime(2022, 9, 5),   # Labor Day
    datetime(2022, 11, 11), # Veterans Day
    datetime(2022, 11, 24), # Thanksgiving
    datetime(2022, 12, 25), # Christmas
    # 2023
    datetime(2023, 1, 1),
    datetime(2023, 1, 16),
    datetime(2023, 2, 20),
    datetime(2023, 5, 29),
    datetime(2023, 6, 19),
    datetime(2023, 7, 4),
    datetime(2023, 9, 4),
    datetime(2023, 11, 10), # Veterans Day observed
    datetime(2023, 11, 23),
    datetime(2023, 12, 25),
    # 2024
    datetime(2024, 1, 1),
    datetime(2024, 1, 15),
    datetime(2024, 2, 19),
    datetime(2024, 5, 27),
    datetime(2024, 6, 19),
    datetime(2024, 7, 4),
    datetime(2024, 9, 2),
    datetime(2024, 11, 11),
    datetime(2024, 11, 28),
    datetime(2024, 12, 25),
    # 2025
    datetime(2025, 1, 1),
    datetime(2025, 1, 20),
    datetime(2025, 2, 17),
    datetime(2025, 5, 26),
]

def is_workday(date):
    return date.weekday() < 5 and date not in us_holidays

def generate_workday_data(n=1000):
    # Adjust department distribution - 65% Sales, rest distributed among others
    departments = ['Sales'] * 650 + ['Inventory'] * 100 + ['HR'] * 50 + ['IT'] * 100 + ['Marketing'] * 50 + ['Finance'] * 50
    random.shuffle(departments)
    
    # Define shift distribution: 40% Morning, 40% Afternoon, 20% Night
    shift_types = ['Morning'] * 400 + ['Afternoon'] * 400 + ['Night'] * 200
    random.shuffle(shift_types)

    # Function to assign role based on department
    def assign_role(department):
        if department == 'Sales':
            return random.choices(
                ['Manager', 'Supervisor', 'Analyst', 'Associate', 'Clerk'],
                weights=[0.02, 0.08, 0.1, 0.2, 0.6]
            )[0]
        else:
            return random.choice(['Manager', 'Supervisor', 'Analyst', 'Associate', 'Clerk'])

    data = {
        'employee_id': range(1001, 1001 + n),
        'first_name': [],
        'last_name': [fake.last_name() for _ in range(n)],
        'gender': [],
        'age': [],
        'department': departments,
        'job_role': [assign_role(dept) for dept in departments],
        'shift_type': shift_types,
        'hire_date': [],
        'termination_date': [None] * n,
        'onleave_date': [None] * n,
        'salary': [],
        'location': [random.choice(['New York', 'Los Angeles', 'Chicago', 'Houston', 'Miami']) for _ in range(n)],
        'status': [],
        'performance_score': [],
        'high_absenteeism': [False] * n  # New field to track high absenteeism
    }

    for i in range(n):
        gender = random.choice(['Male', 'Female'])
        data['gender'].append(gender)
        data['first_name'].append(random.choice(male_names if gender == 'Male' else female_names))
        role = data['job_role'][i]
        age_range = role_age_ranges[role]
        age = random.randint(age_range[0], age_range[1])
        data['age'].append(age)
        
        # Base salary calculation
        base_salary = {
            'Manager': random.uniform(80000, 120000),
            'Supervisor': random.uniform(60000, 90000),
            'Analyst': random.uniform(50000, 80000),
            'Associate': random.uniform(40000, 60000),
            'Clerk': random.uniform(30000, 50000)
        }[role]
        
        # Adjust salary based on age and role
        salary = round(base_salary * (1 + (age - 18) * 0.01), 2)
        
        # hire_date entre DATA_START_DATE y DATA_END_DATE - 30 días
        min_hire = DATA_START_DATE
        max_hire = DATA_END_DATE - timedelta(days=30)
        hire_date = fake.date_between_dates(date_start=min_hire, date_end=max_hire)
        data['hire_date'].append(hire_date)
        
        # Ajusta termination_date y onleave_date para no salir del rango
        hire_date_dt = datetime.combine(hire_date, datetime.min.time())
        days_since_hire = (DATA_END_DATE - hire_date_dt).days
        
        # Termination probability based on role and department
        termination_prob = 0.15  # Base probability
        
        # Factors increasing termination probability
        if role == 'Clerk' and data['department'][i] == 'Sales':
            termination_prob *= 1.5  # Higher turnover for sales clerks
        if age < 25:
            termination_prob *= 1.3  # Younger employees more likely to leave
        
        if random.random() < termination_prob:
            data['status'].append('Terminated')
            max_days = min(365 * 5, days_since_hire)
            if max_days < 30:
                max_days = 30
            performance_factor = random.random() * 0.5
            upper_limit = int(max_days * (1 - performance_factor))
            if upper_limit < 30:
                upper_limit = 30
            days_employed = random.randint(30, upper_limit)
            term_date = hire_date + timedelta(days=days_employed)

            if isinstance(term_date, datetime):
                term_date_cmp = term_date.date()
            else:
                term_date_cmp = term_date
            if isinstance(DATA_END_DATE, datetime):
                data_end_cmp = DATA_END_DATE.date()
            else:
                data_end_cmp = DATA_END_DATE
            if term_date_cmp > data_end_cmp:
                term_date = DATA_END_DATE
            data['termination_date'][i] = term_date
            
            # Mark some terminated employees as having high absenteeism (especially in Sales)
            if data['department'][i] == 'Sales' and random.random() < 0.3:  # 30% of terminated sales
                data['high_absenteeism'][i] = True
            
            # Reduce salary for terminated employees
            salary *= random.uniform(0.85, 0.95)
        elif random.random() < 0.1:
            data['status'].append('On Leave')
            max_days = days_since_hire
            if max_days < 30:
                max_days = 30
            days_employed = random.randint(30, max_days)
            leave_date = hire_date + timedelta(days=days_employed)
            # --- Fix: compare .date() ---
            if isinstance(leave_date, datetime):
                leave_date_cmp = leave_date.date()
            else:
                leave_date_cmp = leave_date
            if isinstance(DATA_END_DATE, datetime):
                data_end_cmp = DATA_END_DATE.date()
            else:
                data_end_cmp = DATA_END_DATE
            if leave_date_cmp > data_end_cmp:
                leave_date = DATA_END_DATE
            data['onleave_date'][i] = leave_date
        else:
            data['status'].append('Active')
        
        data['salary'].append(round(salary, 2))
        
        # Generate performance score with bias based on termination status
        if data['status'][i] == 'Terminated':
            # Terminated employees more likely to have lower scores
            score = random.choices(
                [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                weights=[0.15, 0.2, 0.2, 0.15, 0.1, 0.08, 0.06, 0.04, 0.01, 0.01]
            )[0]
        else:
            # Active employees more likely to have higher scores
            score = random.choices(
                [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                weights=[0.01, 0.02, 0.05, 0.08, 0.1, 0.15, 0.2, 0.2, 0.1, 0.09]
            )[0]
        
        data['performance_score'].append(score)

    df = pd.DataFrame(data)
    df['hire_date'] = pd.to_datetime(df['hire_date'])
    df['termination_date'] = pd.to_datetime(df['termination_date'])
    df['onleave_date'] = pd.to_datetime(df['onleave_date'])
    df.to_csv('workday_data.csv', index=False)
    return df

def generate_kronos_data(workday_df=None):
    employee_ids = workday_df['employee_id'].tolist()
    start_date = DATA_START_DATE
    end_date = DATA_END_DATE
    data = {
        'entry_id': [],
        'employee_id': [],
        'work_date': [],
        'hours_worked': [],
        'shift_type': [],
        'overtime_hours': [],
        'original_shift_type': []  
    }

    entry_count = 0
    for emp_id in employee_ids:
        emp_data = workday_df[workday_df['employee_id'] == emp_id].iloc[0]
        hire_date = emp_data['hire_date']
        termination_date = emp_data['termination_date']
        onleave_date = emp_data['onleave_date']
        department = emp_data['department']
        employee_shift = emp_data['shift_type']
        is_high_absentee = emp_data['high_absenteeism']

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
                data['original_shift_type'].append(employee_shift)  # Siempre guarda el turno real

                # Adjust absence probability based on shift type and high absenteeism status
                base_absence_prob = {
                    'Morning': 0.015,  # 1.5% absence
                    'Afternoon': 0.02,  # 2% absence
                    'Night': 0.04  # 4% absence
                }[employee_shift]
                
                # Increase absence probability for high absenteeism employees (mostly in Sales)
                if is_high_absentee:
                    base_absence_prob *= 3  # 3x higher absence rate
                
                if random.random() < base_absence_prob:
                    data['hours_worked'].append(0.0)
                    data['overtime_hours'].append(0.0)
                    data['shift_type'].append('None')
                else:
                    data['hours_worked'].append(round(random.uniform(4, 8), 2))
                    data['shift_type'].append(employee_shift)
                    
                    # Calculate overtime with boosters
                    month = current_date.month
                    multiplier = overtime_multipliers.get(department, {'default': 1.0}).get(
                        'June' if month == 6 else 'December' if month == 12 else 'default', 1.0)
                    
                    # Base overtime probability (8% normally, 15% during boost months)
                    overtime_prob = 0.15 if month in [6, 12] else 0.08
                    
                    if random.random() < overtime_prob:
                        # Higher overtime during boost months
                        overtime_max = 6 if month in [6, 12] else 4
                        overtime = round(random.uniform(0, overtime_max) * multiplier, 2)
                    else:
                        overtime = 0
                    data['overtime_hours'].append(overtime)
                entry_count += 1
            current_date += timedelta(days=1)

    df = pd.DataFrame(data)
    df['work_date'] = pd.to_datetime(df['work_date'])
    df.to_csv('kronos_data.csv', index=False)
    return df

def generate_survey_data(n=2000, workday_df=None):
    employee_ids = workday_df['employee_id'].tolist()
    
    # Score distribution with more weight to positive scores
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
        "Management is {decent} ocasionaly.",
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
        'survey_id': [],
        'employee_id': [],
        'survey_date': [],
        'satisfaction_score': [],
        'response': []
    }

    survey_id_counter = 1
    # Primero, aseguramos al menos una encuesta y evaluación por año de empleo
    for idx, emp_row in workday_df.iterrows():
        emp_id = emp_row['employee_id']
        hire_date = emp_row['hire_date']
        termination_date = emp_row['termination_date']
        status = emp_row['status']
        # Limita fechas al rango global
        if pd.notnull(termination_date):
            end_date = min(termination_date, DATA_END_DATE)
        else:
            end_date = DATA_END_DATE
        if hire_date > end_date:
            end_date = hire_date
        # Calcular años completos de empleo (al menos 1)
        months_employed = max(1, ((end_date.year - hire_date.year) * 12 + (end_date.month - hire_date.month)))
        years_employed = max(1, int(np.ceil(months_employed / 12)))
        # Si fue terminado antes de 6 meses, solo una encuesta en la fecha de terminación
        if pd.notnull(termination_date) and (termination_date - hire_date).days < 180:
            survey_dates = [termination_date]
        else:
            # Una encuesta por año, distribuidas uniformemente
            survey_dates = []
            for y in range(years_employed):
                survey_year = hire_date.year + y
                survey_dt = hire_date + pd.DateOffset(years=y)
                if survey_dt > end_date:
                    survey_dt = end_date
                if survey_dt > DATA_END_DATE:
                    survey_dt = DATA_END_DATE
                survey_dates.append(survey_dt)
        for survey_dt in survey_dates:
            # Score y respuesta
            if status == 'Terminated' and pd.notnull(termination_date) and (termination_date - hire_date).days < 180:
                # Si fue terminado antes de 6 meses, score bajo
                score = random.choice([1, 2, 2, 3])
            else:
                # Score según lógica original
                original_score = random.choices([5, 4, 3, 2, 1], weights=[0.35, 0.35, 0.2, 0.05, 0.05])[0]
                # Ajuste por antigüedad
                months_since_hire = (survey_dt.year - hire_date.year) * 12 + (survey_dt.month - hire_date.month)
                if status == 'Terminated':
                    score = max(1, original_score - random.choice([0, 0, 1, 1, 2]))
                else:
                    if months_since_hire > 12:
                        score = min(5, original_score + random.choice([0, 0, 1]))
                    elif months_since_hire > 6:
                        score = min(5, original_score + random.choice([0, 0, 0, 1]))
                    else:
                        score = original_score
            data['survey_id'].append(survey_id_counter)
            data['employee_id'].append(emp_id)
            data['survey_date'].append(survey_dt)
            data['satisfaction_score'].append(score)
            data['response'].append(generate_response(score))
            survey_id_counter += 1

    # Luego, si faltan encuestas para llegar a n, genera aleatorias (sin violar reglas)
    extra_needed = n - len(data['survey_id'])
    if extra_needed > 0:
        for _ in range(extra_needed):
            emp_id = random.choice(employee_ids)
            emp_data = workday_df[workday_df['employee_id'] == emp_id].iloc[0]
            hire_date = emp_data['hire_date']
            termination_date = emp_data['termination_date']
            status = emp_data['status']
            if pd.notnull(termination_date):
                end_date = min(termination_date, DATA_END_DATE)
            else:
                end_date = DATA_END_DATE
            if hire_date > end_date:
                survey_dt = hire_date
            else:
                delta_days = (end_date - hire_date).days
                if delta_days < 0:
                    survey_dt = hire_date
                else:
                    survey_dt = hire_date + timedelta(days=random.randint(0, delta_days))
            if survey_dt > DATA_END_DATE:
                survey_dt = DATA_END_DATE
            # Score y respuesta
            if status == 'Terminated' and pd.notnull(termination_date) and (termination_date - hire_date).days < 180:
                score = random.choice([1, 2, 2, 3])
            else:
                original_score = random.choices([5, 4, 3, 2, 1], weights=[0.35, 0.35, 0.2, 0.05, 0.05])[0]
                months_since_hire = (survey_dt.year - hire_date.year) * 12 + (survey_dt.month - hire_date.month)
                if status == 'Terminated':
                    score = max(1, original_score - random.choice([0, 0, 1, 1, 2]))
                else:
                    if months_since_hire > 12:
                        score = min(5, original_score + random.choice([0, 0, 1]))
                    elif months_since_hire > 6:
                        score = min(5, original_score + random.choice([0, 0, 0, 1]))
                    else:
                        score = original_score
            data['survey_id'].append(survey_id_counter)
            data['employee_id'].append(emp_id)
            data['survey_date'].append(survey_dt)
            data['satisfaction_score'].append(score)
            data['response'].append(generate_response(score))
            survey_id_counter += 1

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

    # Calcular por turno usando original_shift_type
    shift_absenteeism = {}
    for shift in ['Morning', 'Afternoon', 'Night']:
        shift_records = kronos_df[(kronos_df['is_weekday']) & (kronos_df['original_shift_type'] == shift)].shape[0]
        shift_absences = kronos_df[(kronos_df['is_weekday']) &
                                   (kronos_df['original_shift_type'] == shift) &
                                   (kronos_df['hours_worked'] == 0) &
                                   (kronos_df['overtime_hours'] == 0)].shape[0]
        shift_percentage = (shift_absences / shift_records) * 100 if shift_records > 0 else 0.0
        shift_absenteeism[shift] = round(shift_percentage, 2)

    return {
        'total_weekday_records': total_weekday_records,
        'absences': absences,
        'absenteeism_percentage': round(absenteeism_percentage, 2),
        'by_shift': shift_absenteeism
    }

# Generate datasets
workday_df = generate_workday_data(1000)
kronos_df = generate_kronos_data(workday_df)
survey_df = generate_survey_data(2000, workday_df)

# Calculate absenteeism
absenteeism_result = calculate_absenteeism(kronos_df)

# Print department and role distribution
print("\nDepartment Distribution:")
print(workday_df['department'].value_counts(normalize=True) * 100)
print("\nRole Distribution in Sales:")
print(workday_df[workday_df['department'] == 'Sales']['job_role'].value_counts(normalize=True) * 100)

# Verify shift distribution in Workday
shift_distribution = workday_df['shift_type'].value_counts(normalize=True) * 100
print("\nShift Distribution in Workday (%):")
print(shift_distribution)

# Verify shift distribution in Kronos (excluding absences)
kronos_shift_distribution = kronos_df[kronos_df['shift_type'] != 'None']['shift_type'].value_counts(normalize=True) * 100
print("\nShift Distribution in Kronos (%, excluding absences):")
print(kronos_shift_distribution)

# Print termination statistics
terminated_df = workday_df[workday_df['status'] == 'Terminated']
print("\nTermination Statistics:")
print(f"Total Terminated: {len(terminated_df)}")
print(f"Terminated in Sales: {len(terminated_df[terminated_df['department'] == 'Sales'])}")
print(f"Terminated Clerks: {len(terminated_df[terminated_df['job_role'] == 'Clerk'])}")
print("\nPerformance Scores for Terminated vs Active:")
print("Terminated:", terminated_df['performance_score'].mean())
print("Active:", workday_df[workday_df['status'] == 'Active']['performance_score'].mean())

# Print overtime statistics
print("\nOvertime Statistics:")
print(f"Overall Overtime Rate: {(kronos_df['overtime_hours'] > 0).mean() * 100:.2f}%")
print(f"June Overtime Rate: {(kronos_df[kronos_df['work_date'].dt.month == 6]['overtime_hours'] > 0).mean() * 100:.2f}%")
print(f"December Overtime Rate: {(kronos_df[kronos_df['work_date'].dt.month == 12]['overtime_hours'] > 0).mean() * 100:.2f}%")

# Print absenteeism statistics
print("\nAbsenteeism Statistics:")
print(f"Total Weekday Records: {absenteeism_result['total_weekday_records']}")
print(f"Absences: {absenteeism_result['absences']}")
print(f"Absenteeism Percentage: {absenteeism_result['absenteeism_percentage']}%")
print("\nAbsenteeism by Shift Type:")
for shift, percentage in absenteeism_result['by_shift'].items():
    print(f"{shift}: {percentage}%")

# Print survey trends
print("\nSurvey Score Trends Over Time:")
survey_df['survey_month'] = survey_df['survey_date'].dt.to_period('M')
monthly_scores = survey_df.groupby('survey_month')['satisfaction_score'].mean()
print(monthly_scores.head(10))

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