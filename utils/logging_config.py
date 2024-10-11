import logging

def setup_logging():
    # Existing logging setup code
    pass

def clean_currency_string(value):
    """
    Cleans a currency string by removing symbols and commas.
    """
    if isinstance(value, str):
        # Remove currency symbols and commas
        value = value.replace('$', '').replace(',', '')
    return value
