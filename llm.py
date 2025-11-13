import json
import subprocess
from typing import List, Dict, Any, Optional 

def create_llm_prompt(company_data: Dict[str, Any], scraped_content: str) -> str:
    """
    Creates a high-quality LLM prompt for conditional matching of company info
    to scraped website content using rules and demonstrations.

    Args:
        company_data: Dict with keys 'company_name', 'company_number', 'postcode', 'sic_codes'
        scraped_content: The Markdown content of the scraped webpage

    Returns:
        A string suitable for input to Gemini / GenAI.
    """
    
    # Format SIC codes
    sic_code_str = ", ".join(company_data['sic_codes']) if company_data['sic_codes'] else "N/A"

    # Truncate content to 15k chars
    scraped_text_trunc = scraped_content[:15000]

    # Construct prompt with company info and scraped webpage content
    prompt = f"""
    Your task is to verify if a scraped webpage belongs to a given company.

    Company info:
    - Name: {company_data['company_name']}
    - Company number: {company_data['company_number']}
    - Postcode: {company_data['postcode']}
    - SIC codes: {sic_code_str}

    Scraped webpage content (truncated):
    {scraped_text_trunc}

    Question: Based on the above content, does this webpage belong to the company? 
    Answer with 'Yes' if it matches, otherwise 'No'. Do NOT provide any URLs.
    """
    return prompt

def run_ollama(prompt: str, model: str = "llama3") -> str:
    """
    Runs a local Ollama LLM model using subprocess.

    Args:
        prompt: The prompt string to send to the LLM.
        model: The name of the LLM model to use.

    Returns:
        The LLM's response as a string.
    """
    try:
        # call LLM model via subprocess
        result = subprocess.run(
            ["ollama", "run", model, "--prompt", prompt],
            capture_output = True, # capture stdout
            text = True, # return output as string
            check = True # raise error if command fails
        )
        # return response as string
        return result.stdout.strip()
    # handle errors
    except subprocess.CalledProcessError as e:
        print(f"Error running Ollama model {model}: {e}")
        return ""

def parse_llm_response(response_text: str):
    if not response_text:
        return "No"
    return "Yes" if response_text.lower().startswith("yes","LINK") else "No"

def main():
    input_data = "hpc_input.json" # input JSON file with company info and scraped data
    output_data = "hpc_results.json" # output JSON file for results
    llm_model = "llama3"
    
    # load input data
    with open(input_data, 'r') as f:
        data = json.load(f)
        
    results = [] # store the results
    
    # iterate over each company in the input data
    for i, item in enumerate(data, 1):
        company_data = item["company_data"]
        scraped_content = item["scraped_content"]
        url = item["url"]
        
        # formulate prompt and get LLM response
        prompt = create_llm_prompt(company_data, scraped_content)
        response = run_ollama(prompt, model = llm_model)
        decision = parse_llm_response(response)
        
        # append result to results list
        results.append({
            "company_name": company_data['company_name'],
            "url": url,
            "response": response,
            "decision": decision
        })
        print(f"Processed {i}/{len(data)}: {url} -> {decision}")
    
    # save results to output JSON file    
    with open(output_data, 'w') as f:
        json.dump(results, f, indent = 2)
        
    print(f"Completed {len(results)} checks. Results saved to {output_data}.")
    
    if __name__ == "__main__":
        main()