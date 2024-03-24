import json
import os
from tenacity import retry, wait_random_exponential, stop_after_attempt

from openai import OpenAI

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

from mistralai.client import MistralClient
from mistralai.models.chat_completion import ChatMessage

mistral_client = MistralClient(api_key=os.getenv("MISTRAL_API_KEY"))

GPT_MODEL = "gpt-3.5-turbo-0125"
MISTRAL_MODEL = "mistral-large-latest"

FUNCTIONS = [{
    "name": "extract_job_posting_metadata",
    "description": "Extract structured metadata from a job posting.",
    "parameters": {
        "type": "object",
        "properties": {
            "tagline": {
                "type":
                "string",
                "description":
                "A one sentence description of the job. It should be catchy, and MUST BE extracted directly from the job posting."
            },
            "locations": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "city": {
                            "type": "string",
                        },
                        "state": {
                            "type": "string",
                        },
                        "country": {
                            "type": "string",
                            "description":
                            "The ISO 3166-1 alpha-2 country code."
                        },
                        "latitude": {
                            "type":
                            "number",
                            "description":
                            "You are allowed to geo-code based on the city, and you should try to, even if you don't know the exact coordinates."
                        },
                        "longitude": {
                            "type": "number"
                        }
                    }
                }
            },
            "salary": {
                "type": "object",
                "properties": {
                    "min": {
                        "type": "number",
                    },
                    "max": {
                        "type": "number",
                    },
                    "currency": {
                        "type": "string",
                        "description": "The ISO 4217 currency code."
                    },
                    "unit": {
                        "type": "string",
                        "enum": ["hourly", "weekly", "monthly", "annually"],
                    }
                }
            },
            "equity": {
                "type": "object",
                "properties": {
                    "min": {
                        "type": "number",
                    },
                    "max": {
                        "type": "number",
                    }
                }
            },
            "office_type": {
                "type": "object",
                "description":
                "It's possible for more than one of these to be true.",
                "properties": {
                    "onsite": {
                        "type": "boolean",
                    },
                    "remote": {
                        "type": "boolean",
                    },
                    "hybrid": {
                        "type": "boolean",
                    },
                    "hybrid_days_per_week": {
                        "type":
                        "number",
                        "description":
                        "Number of in-office days per week if hybrid is true."
                    }
                }
            },
            "timezones": {
                "description":
                "For remote jobs, the timezones an employee is allowed to work from, specified in IANA Time Zone Database format (e.g., 'America/New_York', 'Europe/Paris').",
                "type": "array",
                "items": {
                    "type": "string"
                }
            },
            "job_type": {
                "type":
                "string",
                "enum": [
                    "full-time", "part-time", "contract", "internship",
                    "temporary"
                ],
            },
            "experience_level": {
                "type": "string",
                "enum":
                ["entry-level", "mid-level", "senior-level", "executive"],
            },
            "is_manager": {
                "type": "boolean",
                "description": "Is the job a people management role?"
            },
            "years_experience": {
                "type": "object",
                "properties": {
                    "min": {
                        "type": "number",
                    },
                    "max": {
                        "type": "number",
                    }
                }
            },
            "education_level": {
                "type": "string",
                "enum": ["none", "high-school", "bachelors", "masters", "phd"],
            },
            "industry": {
                "type": "string",
            },
            "hard_skills": {
                "description":
                "Things like programming languages, specific software, tools, etc.",
                "type": "array",
                "items": {
                    "type": "string",
                }
            },
            "soft_skills": {
                "description": "Things like communication, teamwork, etc.",
                "type": "array",
                "items": {
                    "type": "string",
                }
            },
            "languages": {
                "description":
                "The ISO 639-1 codes of languages that are required or preferred for candidates.",
                "type": "array",
                "items": {
                    "type": "string",
                    "description": "The ISO 639-1 language code."
                },
            },
            "job_posting_language": {
                "type": "string",
                "description":
                "The ISO 639-1 language code of the job posting."
            },
            "work_authorization_required": {
                "type":
                "boolean",
                "description":
                "Indicates if specific work authorization is required for the job."
            },
            "work_authorization_types": {
                "type": "array",
                "description":
                "List of specific work authorization types required or accepted for the job.",
                "items": {
                    "type":
                    "string",
                    "enum": [
                        "citizen", "permanent_resident", "work_visa",
                        "student_visa", "temporary_work_visa",
                        "exchange_visitor_visa", "refugee_asylum_permitted",
                        "other"
                    ]
                }
            },
            "visa_sponsorship": {
                "type": "object",
                "properties": {
                    "available": {
                        "type":
                        "boolean",
                        "description":
                        "Indicates if visa sponsorship is available for this job."
                    },
                    "types": {
                        "type": "array",
                        "description":
                        "Specific types of visas the company is willing to sponsor.",
                        "items": {
                            "type":
                            "string",
                            "enum": [
                                "h1b", "h1b_transfer", "h1b_cap_exempt", "j1",
                                "f1_opt", "f1_cpt", "o1", "l1", "other"
                            ]
                        }
                    }
                }
            },
            "work_authorization_notes": {
                "type":
                "string",
                "description":
                "Additional notes regarding work authorization, such as details about 'other' authorization types or sponsorship conditions."
            },
            "certifications": {
                "type": "array",
                "items": {
                    "type": "string",
                },
            },
            "travel_required": {
                "type": "boolean",
                "description": "Does the company require travel for this job?"
            },
            "travel_time_percentage": {
                "type":
                "number",
                "description":
                "For jobs with travel, the percentage of the time the employee will be expected to travel."
            },
            "company_size": {
                "type": "object",
                "properties": {
                    "min": {
                        "type": "number",
                    },
                    "max": {
                        "type": "number",
                    },
                }
            },
            "company_stage": {
                "type": "string",
                "enum": ["startup", "early-stage", "mid-stage", "late-stage"],
            },
            "benefits": {
                "description":
                "Tangible benefits like health insurance, 401k, gym membership, etc.",
                "type": "array",
                "items": {
                    "type": "string",
                }
            },
            "cool_factor": {
                "type":
                "number",
                "description":
                "How cool is the company/role? 0-100, where 0 is not at all cool and 100 is completely cool."
            },
            "tags": {
                "type": "array",
                "items": {
                    "type": "string",
                },
            },
            "additional_fields": {
                "type": "object",
                "description":
                "Additional fields that are not covered by the above.",
                "properties": {
                    "name": {
                        "type": "string",
                    },
                    "value": {
                        "type": "string",
                    }
                }
            }
        },
        "required": [],
    },
}]

TOOLS = [{"type": "function", "function": f} for f in FUNCTIONS]


# @retry(wait=wait_random_exponential(multiplier=1, max=5),
#        stop=stop_after_attempt(3))
def extract_job_posting_metadata(job_posting,
                                 model=GPT_MODEL,
                                 temperature=0.1):

    system_prompt = """
    You are a helpful AI assistant. Your job is to extract structured metadata from a job posting. Please extract as many fields as possible. If you are unsure about a field, you can leave it blank, but don't be afraid to use your best judgement. Then call the function extract_job_posting_metadata with the extracted metadata.
    """.strip()

    messages = [{
        "role": "system",
        "content": system_prompt
    }, {
        "role": "user",
        "content": job_posting
    }]
    completion = client.chat.completions.create(
        model=model,
        messages=messages,
        tools=TOOLS,
        tool_choice={
            "type": "function",
            "function": {
                "name": "extract_job_posting_metadata"
            }
        },
        temperature=temperature,
        random_seed=42)

    # TODO: Error handling
    data = json.loads(
        completion.choices[0].message.tool_calls[0].function.arguments)
    return data, completion


# @retry(wait=wait_random_exponential(multiplier=1, max=5),
#        stop=stop_after_attempt(3))
def extract_job_posting_metadata_mistral(job_posting,
                                         model=MISTRAL_MODEL,
                                         temperature=0.1):

    system_prompt = """
    You are a helpful AI assistant. Your job is to extract structured metadata from a job posting. Please extract as many fields as possible. If you are unsure about a field, you can leave it blank, but don't be afraid to use your best judgement. Then call the function extract_job_posting_metadata with the extracted metadata.
    """.strip()

    messages = [{
        "role": "system",
        "content": system_prompt
    }, {
        "role": "user",
        "content": job_posting
    }]
    completion = mistral_client.chat(model=model,
                                     messages=messages,
                                     tool_choice="any",
                                     tools=TOOLS,
                                     temperature=temperature,
                                     random_seed=42)

    # TODO: Error handling
    data = json.loads(
        completion.choices[0].message.tool_calls[0].function.arguments)
    return data, completion


# @retry(wait=wait_random_exponential(multiplier=1, max=5),
#        stop=stop_after_attempt(3))
def extract_job_posting_metadata_mistral_json(job_posting,
                                              model=MISTRAL_MODEL,
                                              temperature=0.1):

    schema = json.dumps(FUNCTIONS[0]["parameters"]["properties"])
    system_prompt = f"""
    You are a helpful AI assistant. Your job is to extract structured metadata from a job posting. Please extract as many fields as possible. If you are unsure about a field, you can leave it blank, but don't be afraid to use your best judgement. Use the following JSON schema for your output: {schema}
    """.strip()

    messages = [{
        "role": "system",
        "content": system_prompt
    }, {
        "role": "user",
        "content": job_posting
    }]
    completion = mistral_client.chat(model=model,
                                     messages=messages,
                                     response_format={"type": "json_object"},
                                     temperature=temperature,
                                     random_seed=42)

    # TODO: Error handling
    data = json.loads(completion.choices[0].message.content)
    return data, completion

def embed_texts_mistral(texts):
    embeddings_batch_response = mistral_client.embeddings(
        model="mistral-embed",
        input=texts,
    )
    return [x.embedding for x in embeddings_batch_response.data]