import asyncio
import json
import logging
import os
import time

from clients import WYANGovClient, GoogleSheetClient

script_dir = os.path.dirname(os.path.abspath(__file__))
logger = logging.getLogger("leads_generator_logger")
file_handler = logging.FileHandler("leads_generator.log")
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(file_handler)
logger.setLevel(logging.INFO)


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

    last_row = sheet_client.get_last_row()
    async with WYANGovClient(config["chromium_path"], logger, width=1920, height=1920) as client:
        leads = await client.get_code_violations()

        for index, lead in enumerate(leads):
            sheet_client.update_cell(last_row, target_street_col_num, lead["street_name"])
            sheet_client.update_cell(last_row, target_city_col_num, lead["city"])
            sheet_client.update_cell(last_row, target_state_col_num, lead["state"])
            sheet_client.update_cell(last_row, target_zip_col_num, lead["zipcode"])
            sheet_client.update_cell(last_row, contact_street_col_num, lead["street_name"])
            sheet_client.update_cell(last_row, contact_city_col_num, lead["city"])
            sheet_client.update_cell(last_row, contact_state_col_num, lead["state"])
            sheet_client.update_cell(last_row, contact_zip_col_num, lead["zipcode"])
            last_row += 1

            if index % 6 == 0 and index != 0:
                time.sleep(60)

if __name__ == "__main__":
    with open(os.path.join(script_dir, 'config.json'), 'rb') as config_file:
        master_config = json.load(config_file)

    google_sheet_client = GoogleSheetClient(os.path.join(script_dir, "credentials-file.json"), "SAM", logger)
    asyncio.run(get_wyan_code_violation(google_sheet_client, master_config))
