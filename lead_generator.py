import asyncio
import json
import logging
import os

from clients import WYANGovClient, GoogleSheetClient
from datetime import datetime

script_dir = os.path.dirname(os.path.abspath(__file__))
logger = logging.getLogger("leads_generator_logger")
file_handler = logging.FileHandler("leads_generator.log")
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(file_handler)
logger.setLevel(logging.INFO)


def extend_and_add(lst, index, value, filler=""):
    # Extend the list with the filler value up to the required index
    if index >= len(lst):
        lst.extend([filler] * (index - len(lst) + 1))

    # Set the value at the desired index
    lst[index] = value

    return lst


async def get_wyan_code_violation(sheet_client: GoogleSheetClient, config: dict):
    sheet_client.open_sheet('Leads Master')
    target_street_col_num = sheet_client.get_column_index("TargetStreet")
    target_city_col_num = sheet_client.get_column_index("TargetCity")
    target_state_col_num = sheet_client.get_column_index("TargetState")
    target_zip_col_num = sheet_client.get_column_index("TargetZip")
    contact_street_col_num = sheet_client.get_column_index("ContactStreet")
    contact_city_col_num = sheet_client.get_column_index("ContactCity")
    contact_state_col_num = sheet_client.get_column_index("ContactState")
    contact_zip_col_num = sheet_client.get_column_index("ContactZip")
    datetime_added_col_num = sheet_client.get_column_index("DateTimeAdded")
    type_col_num = sheet_client.get_column_index("Type")
    source_col_num = sheet_client.get_column_index("Source")

    existing_leads = sheet_client.read_records()
    last_row = len(existing_leads) + 2

    existing_addresses = [" ".join([existing_lead["TargetStreet"], existing_lead["TargetCity"], existing_lead["TargetState"], str(existing_lead["TargetZip"])]) for existing_lead in existing_leads]
    now = datetime.now()
    values = []
    async with WYANGovClient(config["chromium_path"], logger, width=1920, height=1920) as client:
        leads = await client.get_code_violations()
        for index, lead in enumerate(leads):
            address = " ".join([lead["street_name"], lead["city"], lead["state"], lead["zipcode"]])
            if address not in existing_addresses:
                lead_values = []
                lead_values = await extend_and_add(lead_values, target_street_col_num - 1, lead["street_name"])
                lead_values = await extend_and_add(lead_values, target_city_col_num - 1, lead["city"])
                lead_values = await extend_and_add(lead_values, target_state_col_num - 1, lead["state"])
                lead_values = await extend_and_add(lead_values, target_zip_col_num - 1, lead["zipcode"])
                lead_values = await extend_and_add(lead_values, contact_street_col_num - 1, lead["street_name"])
                lead_values = await extend_and_add(lead_values, contact_city_col_num - 1, lead["city"])
                lead_values = await extend_and_add(lead_values, contact_state_col_num - 1, lead["state"])
                lead_values = await extend_and_add(lead_values, contact_zip_col_num - 1, lead["zipcode"])
                lead_values = await extend_and_add(lead_values, datetime_added_col_num - 1, now.strftime("%m/%d/%Y %H:%M:%S"))
                lead_values = await extend_and_add(lead_values, type_col_num - 1, "CV")
                lead_values = await extend_and_add(lead_values, source_col_num - 1, "Bob")
                values.append(lead_values)
        sheet_client.sheet.update(values, f"A{last_row}")


if __name__ == "__main__":
    with open(os.path.join(script_dir, 'config.json'), 'rb') as config_file:
        master_config = json.load(config_file)

    google_sheet_client = GoogleSheetClient(os.path.join(script_dir, "credentials-file.json"), "SAM", logger)
    asyncio.run(get_wyan_code_violation(google_sheet_client, master_config))
