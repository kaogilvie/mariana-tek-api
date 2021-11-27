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

        self.api_object = 'ADMIN'

        self.logger = logging.getLogger(__name__)

        self.model_columns = {}
        self.data = []

    def __str__(self):
        return f"{type(self).__name__}"

    def __repr__(self):
        if self.api_key:
            api_key = "hidden"
        return f"{type(self).__name__}(api_key={api_key}, base_url={self.base_url})"

    def get(self, page=False):
        api_call_url = f'{self.base_url}/api/{self.api_object}/?page_size=100'
        if page:
            api_call_url = f'{api_call_url}&page={page}'
        self.logger.info(f"Getting {self.api_object} results from {api_call_url}")

        response = requests.get(api_call_url, headers=self.headers)
        setattr(self, f'{self.api_object}_results', response)
        setattr(self, f'{self.api_object}_json', json.loads(response.content))

    def prep_parse(self):
        if not hasattr(self, f"{self.api_object}_json"):
            raise AttributeError(f"{self.api_object}_json attribute has not been initialized. Try calling the get_class_sessions method first.")

        json_dict = getattr(self, f"{self.api_object}_json")

        self.num_pages = json_dict['meta']['pagination']['pages']
        self.total_results = json_dict['meta']['pagination']['count']
        self.page_counter = 1

        self.logger.info(f"Formatting {self.total_results} from the {self.api_object} result.")
        self.data = []

    def convert_none_strings(self, dictionary):
        for key, value in dictionary.items():
            if value == 'None':
                dictionary['key'] = None
        return dictionary

class BillingAddresses(AdminClient):
    '''Model of aggregated billing addresses from API.'''
    def __init__(self):
        super().__init__()

        self.api_object = 'billing_addresses'

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

    def parse(self):
        self.prep_parse()
        while self.page_counter <= self.num_pages:
            page_payload = getattr(self, f"{self.api_object}_json")
            for entry_dict in page_payload['data']:
                self.data.append({
                    'type': entry_dict['type'],
                    'billing_address_id': entry_dict['id'],
                    **entry_dict['attributes']
                })
            self.page_counter += 1

            self.get(page=self.page_counter)

        self.logger.info(f"Formatted {len(self.data)} entries.")

class ClassSessions(AdminClient):
    def __init__(self):
        super().__init__()

        self.model_columns = {
            'class_session_id': 'int',
            'start_date': 'date',
            'start_datetime': 'timestamp',
            'end_datetime': 'timestamp',
            'capacity': 'int',
            'cancellation_datetime': 'timestamp',
            'instructor_names': 'json',
            'public_waitlist_count': 'int',
            'reservations': 'json'
        }
        self.api_object = 'class_sessions'

    def parse(self):
        ### should be able to make this completely generic
        self.prep_parse()
        while self.page_counter <= self.num_pages:
            page_payload = getattr(self, f"{self.api_object}_json")
            for entry_dict in page_payload['data']:
                dictionary_to_append = {
                    'type': entry_dict['type'],
                    'class_session_id': entry_dict['id'],
                    **entry_dict['attributes'],
                    'reservations': [k['id'] for k in entry_dict['relationships']['reservations']['data']]
                }
                self.data.append(self.convert_none_strings(dictionary_to_append))

            self.page_counter += 1
            self.get(page=self.page_counter)

        self.logger.info(f"Formatted {len(self.data)} entries.")

class Reservations(AdminClient):
    def __init__(self):
        super().__init__()

        self.model_columns = {
            'formatted_address': 'json'
        }
        self.api_object = 'reservations'

    def parse(self):
        self.prep_parse()
        page_payload = getattr(self, f"{self.api_object}_json")
        while self.page_counter <= self.num_pages:
            for entry_dict in page_payload['data']:
                self.data.append({
                    'type': entry_dict['type'],
                    'billing_address_id': entry_dict['id'],
                    **entry_dict['attributes']
                })
            self.page_counter += 1
            self.get(page=self.page_counter)

        self.logger.info(f"Formatted {len(self.data)} entries.")

# ba = ClassSessions()
# ba.get()
# ba.parse()
#
# ba.data[0].keys()
