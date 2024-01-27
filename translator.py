"""
Microsoft Translator Text REST API documentation:
https://learn.microsoft.com/en-us/azure/ai-services/translator/reference/rest-api-guide?WT.mc_id=Portal-Microsoft_Azure_ProjectOxford
"""
import requests
import uuid
from config import config


class Translator:
    """
    A class for interacting with the Microsoft Translator Text API to translate text from one language to another.

    Attributes:
        key (str): The API key for authentication.
        endpoint (str): The endpoint URL for the Translator API.
        location (str): The location or region of the resource.
        path (str): The API path for the translation operation.
        constructed_url (str): The full URL for the translation operation.
        headers (dict): HTTP headers for the API request.

    Methods:
        translate(text, from_lang='he', to_lang='en'): Translates the input text from the source language to the target language.

    """

    def __init__(self):
        """
        Initializes the Translator object with API key, endpoint, location, and other required parameters.
        """
        self.key = config['TRANSLATOR']['KEY']
        self.endpoint = config['TRANSLATOR']['ENDPOINT']
        self.location = config['TRANSLATOR']['LOCATION']
        self.path = '/translate'
        self.constructed_url = self.endpoint + self.path

        self.headers = {
            'Ocp-Apim-Subscription-Key': self.key,
            'Ocp-Apim-Subscription-Region': self.location,
            'Content-type': 'application/json',
            'X-ClientTraceId': str(uuid.uuid4())
        }

    def translate(self, text: str, from_lang='he', to_lang='en') -> str:
        """
        Translates the input text from the source language to the target language.

        Args:
        - text (str): The text to be translated.
        - from_lang (str): The source language code (default is 'he' for Hebrew).
        - to_lang (str): The target language code (default is 'en' for English).

        Returns:
        - str: The translated text.

        Example:
        translator = Translator()
        translated_text = translator.translate('שלום')
        print(f'Translation: {translated_text}')
        """
        params = {
            'api-version': '3.0',
            'from': from_lang,
            'to': [to_lang]
        }
        body = [{'text': text}]
        request = requests.post(self.constructed_url, params=params, headers=self.headers, json=body)
        response = request.json()
        translations = response[0]['translations']
        if translations:
            return translations[0]['text']
        else:
            return ''


# Create a Translator object instance
translator = Translator()

# Define a translate function for convenience
translate = translator.translate
