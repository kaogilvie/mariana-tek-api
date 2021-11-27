# MARIANA TEK API PYTHON SDK
Made with love in house at &KO.

## Fine Print
This SDK is geared towards data extraction and modeling rather than
web application development. As such, some of the supported platforms
and use cases are rather narrow.

Python 3.9+ support guaranteed; Python 3.6 support likely (uses f-strings).
Python 2 is not supported by this library.

Postgres support baked in for the datastore functionality but extending
for other datastores should just involve creating a new subclass in the same style.
Only Postgres 9.5 and up is supported (leverages ON CONFLICT).

Currently, the API key is auto-retrieved from the environment
variable "MARIANA_TEK_API_KEY", but you can override this by passing
an API key to the constructor argument.
Same goes for the base_url (MARIANA_TEK_BASE_URL).

Model columns are contained in the constructor of all raw object retrievals
as a hedge against an API change, since the payloads are unpacked using the
splat operators. You can exclude columns from upload into the database of your
choice by commenting them out or removing them from the constructor, or overriding
`model_columns` after initializing the object.
