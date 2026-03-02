import httpx

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

PROVIDERS = {
    "groq": {
        "name": "Groq",
        "base_url": "https://api.groq.com/openai/v1",
        "model": "llama-3.3-70b-versatile",
        "key_prefix": "gsk_",
    },
    "together": {
        "name": "Together AI",
        "base_url": "https://api.together.xyz/v1",
        "model": "meta-llama/Llama-3.3-70B-Instruct-Turbo",
        "key_prefix": "",
    },
    "openrouter": {
        "name": "OpenRouter",
        "base_url": "https://openrouter.ai/api/v1",
        "model": "meta-llama/llama-3.3-70b-instruct:free",
        "key_prefix": "sk-or-",
    },
}


def detect_provider(api_key: str) -> str:
    """Auto-detect provider from API key prefix."""
    if api_key.startswith("gsk_"):
        return "groq"
    if api_key.startswith("sk-or-"):
        return "openrouter"
    # Together keys don't have a standard prefix, default to together for unknown
    return "together"


def generate_schema_info(columns: list[dict]) -> str:
    schema_parts = []
    for col in columns:
        name = col.get("name", "")
        dtype = col.get("dtype", "")
        sample = col.get("sample", "")
        schema_parts.append(f"- {name}: {dtype} (sample: {sample})")
    return "\n".join(schema_parts)


def nl_to_pandas(question: str, dataset_schema: list[dict], api_key: str | None = None) -> str:
    key = api_key or settings.GROQ_API_KEY
    if not key:
        raise ValueError(
            "No API key provided. Enter your API key in Settings (Groq, Together AI, or OpenRouter)."
        )

    provider_id = detect_provider(key)
    provider = PROVIDERS[provider_id]

    schema_info = generate_schema_info(dataset_schema)

    user_prompt = f"""Dataset schema:
{schema_info}

Question: {question}

Generate the Pandas code to answer this question:"""

    with httpx.Client(timeout=30.0) as client:
        response = client.post(
            f"{provider['base_url']}/chat/completions",
            headers={
                "Authorization": f"Bearer {key}",
                "Content-Type": "application/json",
            },
            json={
                "model": provider["model"],
                "messages": [
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": user_prompt},
                ],
                "temperature": 0.1,
                "max_tokens": 1024,
            },
        )

    if response.status_code == 401:
        raise ValueError(f"Invalid {provider['name']} API key. Please check your key and try again.")
    if response.status_code == 429:
        raise ValueError(f"{provider['name']} rate limit exceeded. Please wait a moment and try again.")
    response.raise_for_status()

    data = response.json()
    code = data["choices"][0]["message"]["content"]
    if not code:
        raise ValueError(f"Empty response from {provider['name']}")

    code = code.strip()

    if code.startswith("```python"):
        code = code[9:]
    elif code.startswith("```"):
        code = code[3:]

    if code.endswith("```"):
        code = code[:-3]

    return code.strip()
