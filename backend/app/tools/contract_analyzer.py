import os
from dotenv import load_dotenv
from groq import Groq

load_dotenv(os.path.expanduser("~/hackblr-legal-ai/.env"))
groq_client = Groq(api_key=os.getenv("GROQ_API_KEY"))

def analyze_contract(contract_text: str) -> dict:
    response = groq_client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[
            {"role": "system", "content": "You are an expert Indian legal contract analyst. Be specific about Indian laws."},
            {"role": "user", "content": f"""Analyze this contract text. For each clause:
1. Clause type (indemnity, termination, non-compete, IP, payment, confidentiality)
2. Risk level: HIGH / MEDIUM / LOW
3. Brief risk explanation
4. Which Indian law governs it
5. Suggested revision if HIGH risk

Contract text:
{contract_text}

Return structured analysis with each clause numbered."""}
        ],
        temperature=0.2,
        max_tokens=2000
    )
    return {
        "analysis": response.choices[0].message.content,
        "model": "llama-3.3-70b",
        "disclaimer": "AI-generated analysis, not legal advice. Consult a qualified lawyer."
    }
