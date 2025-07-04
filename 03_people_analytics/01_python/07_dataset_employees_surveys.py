import pandas as pd
import random
import uuid

# Define sentiment distribution
TOTAL_COMMENTS = 5000
POSITIVE_COUNT = int(TOTAL_COMMENTS * 0.65)  # 3250
NEUTRAL_COUNT = int(TOTAL_COMMENTS * 0.15)  # 750
NEGATIVE_COUNT = TOTAL_COMMENTS - POSITIVE_COUNT - NEUTRAL_COUNT  # 1000

# Templates for comments
positive_templates = [
    "I really enjoy working here; the team is supportive and the environment is great.",
    "The management is excellent and provides clear guidance.",
    "I feel valued and appreciated for my contributions.",
    "The company offers great opportunities for career growth.",
    "Work-life balance is well-maintained, and I feel motivated.",
    "The workplace culture is inclusive and collaborative.",
    "I’m happy with the resources and tools provided to do my job.",
    "My manager listens to my feedback and acts on it.",
    "The company truly cares about employee well-being.",
    "I’m excited to come to work every day!"
]

neutral_templates = [
    "The job is okay, but there’s room for improvement in communication.",
    "Work environment is decent, nothing exceptional.",
    "I neither love nor dislike my role; it’s just fine.",
    "Management is alright, but could be more proactive.",
    "The workload is manageable, but benefits could be better.",
    "I don’t have strong feelings about the company culture.",
    "My role is satisfactory, but I’d like more challenges.",
    "Things are going fine, no major complaints.",
    "The company is average in terms of employee support.",
    "It’s a stable job, but not particularly exciting."
]

negative_templates = [
    "I feel overworked and underappreciated in my role.",
    "Management lacks transparency and doesn’t listen to employees.",
    "The work environment is stressful and chaotic.",
    "There are limited opportunities for career advancement.",
    "The company doesn’t value employee feedback.",
    "Work-life balance is poor, and I feel burned out.",
    "Resources and tools are outdated and hinder productivity.",
    "The culture here is toxic and demotivating.",
    "I’m dissatisfied with the lack of support from leadership.",
    "Morale is low, and I dread coming to work."
]

# Function to generate a comment with variations
def generate_comment(template_list, sentiment, score):
    template = random.choice(template_list)
    # Add some variation by modifying words or phrases
    variations = {
        "great": ["wonderful", "fantastic", "amazing"],
        "supportive": ["helpful", "encouraging", "cooperative"],
        "management": ["leadership", "supervisors", "managers"],
        "environment": ["workplace", "atmosphere", "setting"],
        "poor": ["bad", "terrible", "awful"],
        "stressful": ["overwhelming", "demanding", "tense"]
    }
    comment = template
    for key, options in variations.items():
        if key in comment:
            comment = comment.replace(key, random.choice(options))
    return {"comment": comment, "sentiment": sentiment, "score": score}

# Generate dataset
data = []
# Positive comments (score 0 or 1)
for _ in range(POSITIVE_COUNT):
    score = random.choice([0, 1])
    data.append(generate_comment(positive_templates, "Positive", score))

# Neutral comments (score 2)
for _ in range(NEUTRAL_COUNT):
    data.append(generate_comment(neutral_templates, "Neutral", 2))

# Negative comments (score 3 or 4)
for _ in range(NEGATIVE_COUNT):
    score = random.choice([3, 4])
    data.append(generate_comment(negative_templates, "Negative", score))

# Shuffle the dataset
random.shuffle(data)

# Create DataFrame
df = pd.DataFrame(data, columns=["comment", "sentiment", "score"])

# Save to CSV
df.to_csv("employee_satisfaction_dataset.csv", index=False)

print(f"Dataset generated with {POSITIVE_COUNT} positive, {NEUTRAL_COUNT} neutral, and {NEGATIVE_COUNT} negative comments.")
print("Saved to 'employee_satisfaction_dataset.csv'")