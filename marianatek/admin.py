'''Interface to the Admin API.'''

import os
import json
import logging

import requests

class AdminClient(object):
    '''Base client for connecting to the Admin API.

        Model columns and data list are required for
        upload via the datastore module.
    '''
    def __init__(self, api_key=False, base_url=False):
        self.api_key = api_key
        if self.api_key is False:
            self.api_key = os.environ['MARIANA_TEK_API_KEY']

        self.base_url = base_url
        if self.base_url is False:
            self.base_url = os.environ['MARIANA_TEK_BASE_URL']

        self.headers = {'Authorization': f'Bearer {self.api_key}'}

        self.logger = logging.getLogger(__name__)

        self.model_columns = {}
        self.data = []

    def __str__(self):
        return f"{type(self).__name__}"

    def __repr__(self):
        if self.api_key:
            api_key = "hidden"
        return f"{type(self).__name__}(api_key={api_key}, base_url={self.base_url})"

class BillingAddresses(AdminClient):
    '''Model of aggregated billing addresses from API.'''
    def __init__(self):
        super().__init__()

        self.model_columns = {
            'billing_address_id': 'int',
            'first_name': 'varchar',
            'last_name': 'varchar',
            'address_line1': 'varchar',
            'address_line2': 'varchar',
            'address_line3': 'varchar',
            'city': 'varchar',
            'state_province': 'varchar',
            'postal_code': 'varchar',
            'address_sorting_code': 'varchar',
            'country': 'varchar',
            'formatted_address': 'json'
        }

    def get_billing_addresses(self, page:int=False):
        api_call_url = f'{self.base_url}/api/billing_addresses/?page_size=100'
        if page:
            api_call_url = f'{api_call_url}&page={page}'
        self.logger.info(f"Getting billing address results from {api_call_url}")

        self.billing_address_results = requests.get(api_call_url, headers=self.headers)
        self.billing_address_json = json.loads(self.billing_address_results.content)

    def parse_billing_address_json(self):
        if not hasattr(self, 'billing_address_json'):
            raise AttributeError(f"billing_address_json attribute has not been initialized. Try calling the get_billing_addresses method first.")

        num_pages = self.billing_address_json['meta']['pagination']['pages']
        total_results = self.billing_address_json['meta']['pagination']['count']
        page_counter = 1

        self.logger.info(f"Formatting {total_results} from the billing address result.")
        self.data = []
        while page_counter <= num_pages:
            for user_payload in self.billing_address_json['data']:
                self.data.append({
                    'type': user_payload['type'],
                    'billing_address_id': user_payload['id'],
                    **user_payload['attributes']
                })
            page_counter += 1
            self.get_billing_addresses(page=page_counter)

        self.logger.info(f"Formatted {len(self.data)} entries.")
