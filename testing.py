import helperFile
from transformers import pipeline
import re


def extract_family_name(text):
    family_names = helperFile.get_product_families()

    # Convert text to lowercase for case insensitivity
    text_lower = text.lower()

    # Initialize family_name as None
    family_details = {
        'family_id': None,
        'family_name': None
    }

    # Check for each family_name in the DataFrame
    for index, row in family_names.iterrows():
        if row['family_name'].lower() in text_lower:
            family_details['family_id'] = row['family_id']
            family_details['family_name'] = row['family_name']
            break  # Break out of loop once family_name is found

    return family_details

def extract_years(text):
    # Regular expression pattern to match a year (YYYY)
    pattern = r'\b\d{4}\b'

    # Find all occurrences of the pattern in the text
    matches = re.findall(pattern, text)

    # Convert matched strings to integers (years)
    years = [int(match) for match in matches]

    return years

def extract_store_number(text):
    # Regular expression pattern to match the store number
    store_pattern = r'store (\d+)|store (one|two|three|four|five|six|seven|eight|nine|ten)|store ((?:twenty|thirty|forty|fifty|sixty|seventy|eighty|ninety)(?:\s+(?:one|two|three|four|five|six|seven|eight|nine))?)'

    # Define a mapping for textual representations to numeric values
    word_to_num = {
        'one': 1, 'two': 2, 'three': 3, 'four': 4, 'five': 5,
        'six': 6, 'seven': 7, 'eight': 8, 'nine': 9, 'ten': 10,
        'twenty': 20, 'thirty': 30, 'forty': 40, 'fifty': 50,
        'sixty': 60, 'seventy': 70, 'eighty': 80, 'ninety': 90
    }

    # Find all occurrences of the store number pattern in the text
    matches = re.findall(store_pattern, text.lower())

    # Initialize store number as None
    store_number = None

    # Process matches
    for match in matches:
        if match[0]:  # Numeric representation found
            store_number = int(match[0])  # Convert numeric string to integer
            break
        elif match[1]:  # Textual representation found (one to ten)
            store_number = word_to_num.get(match[1])  # Map text to numeric value
            break
        elif match[2]:  # Textual representation found (twenty to ninety)
            # Split the match into parts and map each part to its numeric value
            parts = match[2].split()
            tens = word_to_num.get(parts[0])
            ones = word_to_num.get(parts[1]) if len(parts) > 1 else 0
            store_number = tens + ones
            break

    return store_number

# Example usage:
if __name__ == "__main__":
    print("Hello Testing")
    # Input text
    # text1 = "give me sales of automotive of Store forty eight in 2017"
    #
    # # Extract family_name from text1 using the function
    # family_name = extract_family_name(text1)
    # year = extract_years(text1)
    # store_number = extract_store_number(text1)
    #
    # # Print extracted family_name
    # print(f"Family Name: {family_name} \n\n")
    # print(f"Year: {year} \n\n")
    # print(f"Store Number: {store_number} ")

    # Load a pre-trained model and tokenizer
    # You can choose different models from the Hugging Face model hub

