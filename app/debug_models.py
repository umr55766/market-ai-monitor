import google.generativeai as genai
import os

api_key = os.getenv("GEMINI_API_KEY")
genai.configure(api_key=api_key)

try:
    for m in genai.list_models():
        if "gemini" in m.name and "generateContent" in m.supported_generation_methods:
            print(f"Model: {m.name}")
except Exception as e:
    print(f"Error: {e}")
