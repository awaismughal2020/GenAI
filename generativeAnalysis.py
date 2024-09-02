from transformers import pipeline
import re
import helperFile


def get_family_id(sale_type):
    # Retrieve the product families DataFrame
    product_families = helperFile.get_product_families()

    # Convert the sale type to uppercase to match the family_name format
    sale_type_upper = sale_type.upper()

    # Search for the sale type in the family_name column
    matching_family = product_families[product_families['family_name'] == sale_type_upper]

    # Check if a match was found and return the family_id
    if not matching_family.empty:
        return matching_family['family_id'].values[0]
    else:
        return None

# Load a pre-trained model and tokenizer
extractor = pipeline("text2text-generation", model="google/flan-t5-large")

product_families = helperFile.get_product_families()
family_names = product_families['family_name'].tolist()
family_names_str = ", ".join(family_names).lower()


print("\n\nHere is the list of Sales/Product Types:\n", family_names_str)
print("\n\nStore Number is between: 1-54\n\nSales Records Exist from year 2013 to 2018\n")

input_string = input("Enter your query: ")

# Define prompts to extract each piece of information separately
sum_prompt = (
    f"Does the following string contain the phrase 'sum of'? Answer 'yes' or 'None'.\n\n"
    f"Example 1: 'give me the sum of all expenses' -> yes\n"
    f"Example 2: 'what are the total sales' -> yes\n"
    f"Example 2: 'what are the total of sales' -> yes\n"
    f"Example 3: 'sum of all products' -> yes\n"
    f"Example 4: '{input_string}' -> "
)

sale_type_prompt = (
    f"Extract the sale type (e.g., {family_names_str}) from the following text. "
    f"If there is no sale type mentioned, return 'None'.\n\n"
    f"Example 1: 'give me sum of sales of automobiles in the year 2019' -> automobiles\n"
    f"Example 2: 'how many electronics were sold last month' -> electronics\n"
    f"Example 3: 'what are the total sales' -> None\n"
    f"Example 4: 'sum of groceries sold in store number 10' -> groceries\n\n"
    f"Text: {input_string}\n\n"
    f"Sale Type:"
)

store_number_prompt = (
    f"Extract the store number (if any) which is in the range from 1 to 54 from the following text. "
    f"If there is no store number, return 'None'.\n\n"
    f"Example 1: 'give me sum of sales of automobiles from store number 48 in 2019' -> 48\n"
    f"Example 2: 'give me sum of sales of automobiles in the year 2019' -> None\n"
    f"Example 3: 'sales of automobiles from store number 10 last month' -> 10\n"
    f"Example 4: 'how many cars sold in store 55' -> 55\n"
    f"Example 5: 'store number 25 had highest sales' -> 25\n\n"
    f"Text: {input_string}\n\n"
    f"Store Number:"
)

day_prompt = (
    f"Extract only the day from the following text. The date format is day-month-year. Only provide the day number. "
    f"If there is no day mentioned, return 'None'.\n\n"
    f"Example 1: 'give me sum of sales of automobiles of 12-10-2016' -> 12\n"
    f"Example 2: 'how many electronics were sold last month in 2019' -> None\n"
    f"Example 3: 'what are the total sales for 2020' -> None\n"
    f"Example 4: 'sum of groceries sold on 05-11' -> 05\n\n"
    f"Example 5: 'give me sales of automotive on 10-10-2017' -> 10\n\n"
    f"Text: {input_string}\n\n"
    f"Day:"
)

month_prompt = (
    f"Extract only the month from the following text. The date format is day-month-year. Only provide the month number."
    f"If there is no month mentioned, return 'None'.\n\n"
    f"Example 1: 'give me sum of sales of automobiles of 12-10-2016' -> 10\n"
    f"Example 2: 'how many electronics were sold last month in 2019' -> None\n"
    f"Example 3: 'what are the total sales for 2020' -> None\n"
    f"Example 4: 'sum of groceries sold on 05-11' -> 11\n\n"
    f"Example 5: 'give me sales of automotive on 10-10-2017' -> 10\n\n"
    f"Text: {input_string}\n\n"
    f"Month:"
)

year_prompt = (
    f"Extract only the year from the following text. If there is no year mentioned, return 'None'.\n\n"
    f"Example 1: 'give me sum of sales of automobiles in the year 2019' -> 2019\n"
    f"Example 2: 'how many electronics were sold last month' -> None\n"
    f"Example 3: 'what are the total sales for 2020' -> 2020\n"
    f"Example 4: 'sum of groceries sold in store number 10 last year' -> None\n\n"
    f"Example 5: 'give me sales of automotive on 10-10-2017' -> 2017\n\n"
    f"Text: {input_string}\n\n"
    f"Year:"
)

# Use the model to generate the extracted information
sum_info = extractor(sum_prompt, max_new_tokens=50)
sale_type_info = extractor(sale_type_prompt, max_new_tokens=50)
store_number_info = extractor(store_number_prompt, max_new_tokens=50)
day_info = extractor(day_prompt, max_new_tokens=50)
month_info = extractor(month_prompt, max_new_tokens=50)
year_info = extractor(year_prompt, max_new_tokens=50)

# Extracted information
sum_info_text = sum_info[0]['generated_text'].strip()
sale_type_text = sale_type_info[0]['generated_text'].strip()
store_number_text = store_number_info[0]['generated_text'].strip()
day_text = day_info[0]['generated_text'].strip()
month_text = month_info[0]['generated_text'].strip()
year_text = year_info[0]['generated_text'].strip()

# Display the extracted information
print("\n\nExtracted Sale Type:", sale_type_text)
print("\n\nExtracted Store Number:", store_number_text)
print("\n\nExtracted Day:", day_text)
print("\n\nExtracted Month:", month_text)
print("\n\nExtracted Year:", year_text)
print("\n\nExtracted Sum of:", sum_info_text)

# Get the family ID for the extracted sale type
family_id = get_family_id(sale_type_text)
print("\n\nFamily ID for sale type:", family_id)

# Construct the prompt for the model

return_result = helperFile.get_product_sales_details(family_id, day_text, month_text, year_text, store_number_text, sum_info_text)
return_result = return_result.iloc[0]

prompt = f"""
Based on the provided query and the retrieved result, generate a comprehensive response. Additionally, summarize the findings in 3 to 4 lines:
- Query: {input_string}
- Result: {return_result}

Please include explanations and any relevant context to make the response clear and informative.
"""


# Generate the response using the model
response = extractor(prompt, max_length=100, num_return_sequences=1)

    # Extract the generated text from the response
generated_text = response[0]['generated_text'].strip()


print("\n\nQuery Response:", generated_text)
