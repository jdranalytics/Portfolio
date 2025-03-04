from datasets import load_dataset

    # Load the dataset
dataset = load_dataset("alexcom/analisis-sentimientos-textos-turisitcos-mx-polaridad", split="train")

    # Print dataset information
print(f"Número de comentarios: {len(dataset)}")
print(f"Ejemplo: {dataset[0]}")
print(f"Columns: {dataset.column_names}")
print("First 5 rows of the dataset:")
print(dataset[:5])

