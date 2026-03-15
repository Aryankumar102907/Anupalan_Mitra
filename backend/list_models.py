import os
from dotenv import load_dotenv
load_dotenv()
import google.generativeai as genai

genai.configure(api_key=os.getenv('GOOGLE_API_KEY'))

print(f"{'Model':<45} {'Supported Methods'}")
print('-' * 80)
try:
    for m in genai.list_models():
        methods = ', '.join(m.supported_generation_methods)
        print(f"{m.name:<45} {methods}")
except Exception as e:
    print(f"Error: {e}")
