'''Interface to the Admin API.'''

import os
import json

import requests

class AdminClient(object):
    '''Base client for connecting to the Admin API.'''
    def __init__(self, api_key=False, base_url=False):
        self.api_key = api_key
        if self.api_key is False:
            self.api_key = os.environ['MARIANA_TEK_API_KEY']

        self.base_url = base_url
        if self.base_url is False:
            self.base_url = os.environ['MARIANA_TEK_BASE_URL']

        self.headers = {'Authorization': f'Bearer {self.api_key}'}

class BillingAddresses(AdminClient):
    '''Model of aggregated billing addresses from API.'''
    def __init__(self):
        super().__init__()

        self.model_columns = [
            'billing_address_id',
            'first_name',
            'last_name',
            'address_line1',
            'address_line2',
            'address_line3',
            'city',
            'state_province',
            'postal_code',
            'address_sorting_code',
            'country',
            'formatted_address'
        ]

    def get_billing_addresses(self):
        self.billing_address_results = requests.get(f'{self.base_url}/api/billing_addresses/', headers=self.headers)
        self.billing_address_json = json.loads(self.billing_address_results.content)

    def parse_billing_address_json(self):
        if not hasattr(self, 'billing_address_json'):
            raise AttributeError(f"billing_address_json attribute has not been initialized. Try calling the get_billing_addresses method first.")

        num_pages = self.billing_address_json['meta']['pagination']['pages']
        total_results = self.billing_address_json['meta']['pagination']['count']
        page_counter = 1

        self.billing_addresses = []

        while page_counter <= num_pages:
            for user_payload in self.billing_address_json['data']:
                self.billing_addresses.append({
                    'type': user_payload['type'],
                    'billing_address_id': user_payload['id'],
                    **user_payload['attributes']
                })
            page_counter += 1
