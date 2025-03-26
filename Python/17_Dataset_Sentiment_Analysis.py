import random
import pandas as pd

# Nombre de la empresa ficticia
company_name = "NovaPay"

# Plantillas de comentarios más largos y enriquecidos según el puntaje de sentimiento
templates = {
    1: [
        "I’ve been trying to get help from NovaPay for over a month now, and it’s been an absolute disaster. My issue with a payment glitch still hasn’t been resolved, despite sending multiple emails with all the details they requested. The support team seems completely clueless, and every response feels like a generic copy-paste that doesn’t even address my specific problem. This is beyond frustrating!",
        "The customer service at NovaPay is hands down the worst I’ve ever experienced. I had an urgent issue with a transaction that was incorrectly processed, and it took them three weeks to even acknowledge my request. When they finally replied, it was clear they hadn’t even read my message properly—total incompetence. I’m seriously considering switching to another provider because this is unacceptable.",
        "I reached out to NovaPay’s support team because my account was locked for no apparent reason, and the experience was infuriating. Not only did they fail to provide any clear explanation, but they kept asking for the same documents I’d already sent multiple times. It’s been two weeks, and I still can’t access my funds—what a terrible way to treat customers!",
        "Honestly, I expected better from NovaPay. I contacted their support about a billing error that cost me a significant amount, and all I got was a vague email saying they’re ‘looking into it.’ No updates, no solutions, just empty promises. It’s been a nightmare trying to get any real help from them.",
        "NovaPay’s support is a complete letdown. I had a critical issue with a delayed payout that affected my business, and their team didn’t even bother to respond for over ten days. When they did, it was a robotic message that offered no solution or apology—just pure indifference. I’m so disappointed in their lack of care."
    ],
    2: [
        "I had a mixed experience with NovaPay’s customer support. I reached out about a problem with integrating their payment system into my website, and while they did reply within a few days, the answer wasn’t very helpful. It felt like they were just pointing me to generic help pages instead of addressing my specific situation. It’s not terrible, but it could definitely improve.",
        "The support from NovaPay was underwhelming, to say the least. I asked for assistance with a refund process that wasn’t working correctly, and although they got back to me, the instructions they provided were confusing and didn’t fully resolve the issue. I had to figure out most of it on my own, which wasn’t what I expected.",
        "I contacted NovaPay because I couldn’t understand why a payment was rejected, and their response left me wanting more. They replied after a couple of days with some basic troubleshooting steps, but it didn’t solve my problem. It’s not the worst service, but it’s far from impressive—more effort would’ve been appreciated.",
        "NovaPay’s support team tried to assist me with an account verification issue, but it wasn’t a smooth process. The response took longer than I’d hoped, and when it came, it only partially addressed my concerns. I still had to follow up twice to get things sorted, which made it more tedious than it should’ve been.",
        "My interaction with NovaPay’s support was just okay. I had an issue with a feature not working as advertised, and while they responded, the solution they offered didn’t fully fix it. It’s not a complete failure, but I wish they’d taken more time to understand my needs instead of rushing a half-baked reply."
    ],
    3: [
        "I reached out to NovaPay’s support team about a small glitch in my payment dashboard, and the experience was decent but nothing extraordinary. They got back to me within a reasonable time and provided some general advice that helped a bit, though I still had to tweak things myself. It’s an average service—neither great nor terrible.",
        "NovaPay’s customer service was fine when I asked about updating my account details. They replied after a couple of days with the steps I needed to follow, which worked, but the instructions weren’t as clear as they could’ve been. It’s a neutral experience for me—no major complaints, but no rave reviews either.",
        "The support from NovaPay was alright when I had a question about transaction fees. They responded with the information I asked for, but it didn’t go beyond what I could’ve found online myself. It solved my query, but I wouldn’t say it stood out as exceptional or particularly bad.",
        "I had an issue with a delayed notification from NovaPay, and their support team handled it in a standard way. They replied within a few days, explained the delay, and it was resolved eventually. It wasn’t a frustrating process, but it didn’t leave me overly impressed either—just a middle-of-the-road interaction.",
        "NovaPay’s support team assisted me with a minor login problem, and it was a typical experience. They got back to me with a solution that worked, but it took a little longer than I expected, and the tone was very formal. It’s okay service—does the job but doesn’t wow you."
    ],
    4: [
        "I had a really good experience with NovaPay’s customer support recently. I was struggling to set up a new payment option for my clients, and their team responded within a day with detailed instructions that made the process so much easier. They were polite and proactive, which I really appreciated—solid service overall!",
        "NovaPay’s support team impressed me when I reached out about a payment that didn’t go through. They replied quickly, within hours, and walked me through the steps to fix it, even suggesting a workaround to avoid it in the future. It’s not perfect, but it’s definitely a reliable and friendly service.",
        "I contacted NovaPay because I needed clarification on their fee structure, and I was pleasantly surprised by their support. They got back to me the next day with a thorough explanation and even included examples to make it crystal clear. It’s great to see such attentive care from their team!",
        "The customer service at NovaPay was pretty awesome when I had an issue with a client refund. They responded promptly, offered a step-by-step guide to resolve it, and followed up to ensure everything was okay. It’s not flawless, but it’s definitely a positive experience worth noting.",
        "NovaPay’s support team did a fantastic job helping me with a technical glitch on their platform. I emailed them late at night, and by the next morning, I had a detailed response with a fix that worked perfectly. They’re efficient and courteous—really happy with how they handled it!"
    ],
    5: [
        "I can’t say enough good things about NovaPay’s customer support—it’s absolutely outstanding! I had a complex issue with a large transaction that got stuck, and their team not only replied within hours but also went above and beyond to resolve it quickly and keep me updated throughout. This is what top-tier service looks like!",
        "NovaPay’s support team is phenomenal—I’m genuinely blown away. I reached out about integrating their system with my online store, and they provided a detailed, personalized response within a day, complete with resources and a follow-up to ensure it worked. Hands down the best support I’ve ever received!",
        "The service from NovaPay is exceptional in every way. I had an urgent problem with a payment batch, and their team responded almost instantly with a clear solution, fixed it on their end, and even checked in later to confirm everything was running smoothly. I’m so impressed by their dedication!",
        "NovaPay’s customer support is pure excellence. I was dealing with a confusing account verification process, and they guided me through every step with patience and clarity, resolving it in less than a day. Their professionalism and kindness make them stand out—truly amazing!",
        "I’ve never had such an incredible experience with customer support until NovaPay. I contacted them about a feature request, and not only did they reply promptly with a workaround, but they also escalated it to their development team for future updates. They’re fast, friendly, and genuinely care—five stars all the way!"
    ]
}

# Función para generar variaciones más ricas de un comentario
def generate_variation(comment):
    variations = [
        comment,
        comment.replace("support", "customer service team") if "support" in comment else comment,
        comment.replace("quickly", "in record time") if "quickly" in comment else comment,
        comment.replace("problem", "challenge") if "problem" in comment else comment,
        f"{comment} I’m grateful for their efforts!" if "thank" in comment else comment,
        f"Thanks to {company_name}, this was handled beautifully!" if random.random() > 0.5 else comment,
        f"{comment} It’s clear they value their customers." if random.random() > 0.5 else comment
    ]
    return random.choice(variations)

# Generar el dataset
def create_dataset(size=5000):
    comments = []
    sentiments = []

    # Distribución sesgada hacia sentimientos positivos (4 y 5)
    sentiment_weights = [0.1, 0.15, 0.2, 0.3, 0.25]  # Pesos para 1, 2, 3, 4, 5
    sentiment_values = [1, 2, 3, 4, 5]

    for _ in range(size):
        sentiment = random.choices(sentiment_values, weights=sentiment_weights, k=1)[0]
        base_comment = random.choice(templates[sentiment])
        varied_comment = generate_variation(base_comment)
        comments.append(varied_comment)
        sentiments.append(sentiment)

    # Crear el DataFrame
    dataset = pd.DataFrame({
        "comment": comments,
        "sentiment": sentiments
    })
    return dataset

# Generar el dataset de 10000 comentarios
dataset = create_dataset(10000)

# Verificar la distribución de sentimientos
print("Distribución de sentimientos:")
print(dataset["sentiment"].value_counts().sort_index())

# Guardar el dataset en un archivo CSV
dataset.to_csv("novapay_customer_service_dataset.csv", index=False)
print("\nDataset guardado como 'novapay_customer_service_dataset.csv'")

# Mostrar una muestra de 5 comentarios
print("\nMuestra de 5 comentarios:")
print(dataset.sample(5))