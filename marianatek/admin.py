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

    def get(self, page=False, page_size=100):
        '''
        HTTP call to an API endpoint that results in dynamic setting of the
        result variables to be named after the object being queried.
        '''
        api_call_url = f'{self.base_url}/api/{self.api_object}/?page_size={page_size}'
        if page:
            api_call_url = f'{api_call_url}&page={page}'
        self.logger.info(f"Getting {self.api_object} results from {api_call_url}")

        response = requests.get(api_call_url, headers=self.headers)
        setattr(self, f'{self.api_object}_results', response)
        setattr(self, f'{self.api_object}_json', json.loads(response.content))

    def prep_parse(self):
        '''
        Sets up the necessary data structures to being parsing the payload.
        '''
        if not hasattr(self, f"{self.api_object}_json"):
            raise AttributeError(f"{self.api_object}_json attribute has not been initialized. Try calling the get method first.")

        json_dict = getattr(self, f"{self.api_object}_json")

        self.num_pages = json_dict['meta']['pagination']['pages']
        self.total_results = json_dict['meta']['pagination']['count']
        self.page_counter = 1

        self.logger.info(f"Formatting {self.total_results} from the {self.api_object} result.")
        self.data = []

    def create_relationship_dictionary(self, single_instance_payload):
        '''Flattens relationship portion of payload into key and arrays.
        Does NOT preserve the "type" part of the relationship data structure.
        '''
        flattened_dict = {}
        relationships = single_instance_payload['relationships']
        for key, rel_data in relationships.items():
            if rel_data['data'] is None:
                flattened_dict[key] = None
                continue
            if type(rel_data['data']) is dict:
                flattened_dict[key] = rel_data['data']['id']
            if (type(rel_data['data']) is list) & (len(rel_data['data']) > 0):
                rel_data_list = []
                for entry in rel_data['data']:
                    rel_data_list.append(entry['id'])
                flattened_dict[key] = rel_data_list
        return flattened_dict

    def parse(self, page_size=100):
        '''Retrieve results from API call and formats them.
           Collapses relationships into a flattened dictionary.'''
        self.get(page_size=page_size)

        self.prep_parse()

        while self.page_counter <= self.num_pages:
            page_payload = getattr(self, f"{self.api_object}_json")
            for entry_dict in page_payload['data']:
                rel_dictionary = self.create_relationship_dictionary(entry_dict)
                self.data.append({
                    'type': entry_dict['type'],
                    f'{self.api_object}_id': entry_dict['id'],
                    **entry_dict['attributes'],
                    **rel_dictionary
                })

            self.page_counter += 1
            self.get(page=self.page_counter, page_size=page_size)

        self.logger.info(f"Formatted {len(self.data)} entries.")

class BillingAddresses(AdminClient):
    '''Model of aggregated billing addresses from API.'''
    def __init__(self):
        super().__init__()

        self.model_columns = {
            'billing_address_id': 'integer',
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
            'formatted_address': 'text[]'
        }
        self.api_object = 'billing_addresses'

class ClassSessions(AdminClient):
    '''Model of class sessions from API.'''
    def __init__(self):
        super().__init__()

        self.model_columns = {
            'class_session_id': 'int',
            'start_date': 'date',
            'start_datetime': 'timestamp',
            'end_datetime': 'timestamp',
            'capacity': 'int',
            'cancellation_datetime': 'timestamp',
            'instructor_names': 'text[]',
            'public_waitlist_count': 'int',
            'reservations': 'text[]'
        }
        self.api_object = 'class_sessions'

class Reservations(AdminClient):
    '''Model of reservation data from API.'''
    def __init__(self):
        super().__init__()

        self.model_columns = {
            'reservation_id': 'int',
            'cancel_date': 'date',
            'check_in_date': 'date',
            'guest_name': 'varchar',
            'status': 'varchar',
            'class_session': 'int',
        }
        self.api_object = 'reservations'
