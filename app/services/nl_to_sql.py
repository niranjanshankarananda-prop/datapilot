import httpx

from app.config import settings


SQL_SYSTEM_PROMPT = """You are a SQL expert. Convert natural language questions into SQL queries.

RULES:
1. Generate ONLY valid SQL — no explanations, no markdown
2. Use the exact table name and column names provided
3. Write safe read-only SELECT queries only
4. Use standard SQL compatible with SQLite and PostgreSQL
5. For aggregations always include GROUP BY
6. Alias aggregated columns clearly: SUM(revenue) AS total_revenue
7. Return ONLY the SQL query, nothing else
"""

PROVIDERS = {
    "groq": {
        "base_url": "https://api.groq.com/openai/v1",
        "model": "llama-3.3-70b-versatile",
    },
    "openrouter": {
        "base_url": "https://openrouter.ai/api/v1",
        "model": "meta-llama/llama-3.3-70b-instruct:free",
    },
    "together": {
        "base_url": "https://api.together.xyz/v1",
        "model": "meta-llama/Llama-3.3-70B-Instruct-Turbo",
    },
}


def _detect_provider(api_key: str) -> str:
    if api_key.startswith("gsk_"):
        return "groq"
    if api_key.startswith("sk-or-"):
        return "openrouter"
    return "together"


def _call_llm(prompt: str, api_key: str) -> str:
    provider_id = _detect_provider(api_key)
    provider = PROVIDERS[provider_id]
    with httpx.Client(timeout=30.0) as client:
        response = client.post(
            f"{provider['base_url']}/chat/completions",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={
                "model": provider["model"],
                "messages": [
                    {"role": "system", "content": SQL_SYSTEM_PROMPT},
                    {"role": "user", "content": prompt},
                ],
                "temperature": 0.1,
                "max_tokens": 512,
            },
        )
    if response.status_code == 401:
        raise ValueError("Invalid API key.")
    if response.status_code == 429:
        raise ValueError("Rate limit exceeded. Please wait and try again.")
    response.raise_for_status()
    return response.json()["choices"][0]["message"]["content"].strip()


def nl_to_sql(question: str, schema: list[dict], table_name: str, api_key: str | None = None) -> str:
    """Convert a natural language question to a SQL SELECT query."""
    key = api_key or settings.GROQ_API_KEY
    if not key:
        raise ValueError(
            "No API key provided. Enter your API key in Settings (Groq, Together AI, or OpenRouter)."
        )

    col_descriptions = "\n".join(
        f"  - {col['name']} ({col['dtype']})" for col in schema
    )
    prompt = f"""Table: {table_name}
Columns:
{col_descriptions}

Question: {question}

Write a SQL SELECT query:"""

    sql = _call_llm(prompt, key)

    # Strip markdown fences if the model wrapped the SQL
    if sql.startswith("```"):
        parts = sql.split("```")
        sql = parts[1]
        if sql.lower().startswith("sql"):
            sql = sql[3:]

    return sql.strip()
