from groq import Groq

from app.config import settings


SYSTEM_PROMPT = """You are a data analysis assistant that converts natural language questions into Pandas code.

You must generate ONLY Python Pandas code that operates on a DataFrame variable called 'df'.
The dataset is already loaded into a pandas DataFrame named 'df'.

IMPORTANT RULES:
1. Only use pandas, numpy, and datetime libraries
2. Do NOT import any libraries - assume pandas is available as 'pd' and numpy as 'np'
3. The input DataFrame is already in variable 'df'
4. Always assign your final result to a variable called 'result'
5. For returning dataframes, convert them to dict format using: result = df.to_dict(orient='records')
6. For returning scalar values (like counts, sums, means), assign directly: result = <scalar_value>
7. For returning text/strings: result = <string>

Return ONLY the Python code, no explanations or markdown.
"""


def generate_schema_info(columns: list[dict]) -> str:
    schema_parts = []
    for col in columns:
        name = col.get("name", "")
        dtype = col.get("dtype", "")
        sample = col.get("sample", "")
        schema_parts.append(f"- {name}: {dtype} (sample: {sample})")
    return "\n".join(schema_parts)


def nl_to_pandas(question: str, dataset_schema: list[dict]) -> str:
    client = Groq(api_key=settings.GROQ_API_KEY)

    schema_info = generate_schema_info(dataset_schema)

    user_prompt = f"""Dataset schema:
{schema_info}

Question: {question}

Generate the Pandas code to answer this question:"""

    response = client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0.1,
        max_tokens=1024,
    )

    code = response.choices[0].message.content
    if not code:
        raise ValueError("Empty response from Groq")

    code = code.strip()

    if code.startswith("```python"):
        code = code[9:]
    elif code.startswith("```"):
        code = code[3:]

    if code.endswith("```"):
        code = code[:-3]

    return code.strip()
