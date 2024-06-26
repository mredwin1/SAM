import datetime
import json
import logging
import os
import random
import requests

from clients import GoogleSheetClient, BatchDataClient, DealMachineClient, BatchAPIError

script_dir = os.path.dirname(os.path.abspath(__file__))
logger = logging.getLogger("leads_manager_logger")
file_handler = logging.FileHandler("leads_manager.log")
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(file_handler)
logger.setLevel(logging.INFO)


def validate_lead(lead: dict):
    try:
        if lead["SkipTraceSuccess"] == "" and lead["ContactCity"] and lead["ContactStreet"] and lead["ContactState"] and lead["ContactZip"] and not (lead["ContactPhone1"] and lead["ContactPhone2"] and lead["ContactPhone3"]):
            return True
    except KeyError as e:
        logger.error(e, exc_info=True)

    return False


def extend_and_add(lst, index, value, filler=""):
    # Extend the list with the filler value up to the required index
    if index >= len(lst):
        lst.extend([filler] * (index - len(lst) + 1))

    # Set the value at the desired index
    lst[index] = value

    return lst


def skip_trace(sheet_client: GoogleSheetClient):
    sheet_client.open_sheet("Leads Master")
    contact_phone1_col_num = sheet_client.get_column_index("ContactPhone1")
    contact_phone2_col_num = sheet_client.get_column_index("ContactPhone2")
    contact_phone3_col_num = sheet_client.get_column_index("ContactPhone3")
    skip_trace_result_col_num = sheet_client.get_column_index("SkipTraceSuccess")

    leads = sheet_client.read_records()
    with BatchDataClient(logger, os.path.join(script_dir, "config.json")) as client:
        skip_trace_count = 0
        for index, lead in enumerate(leads):
            row_num = index + 2
            leads_lst = [value for value in lead.values()]
            if validate_lead(lead) and skip_trace_count < 5:
                try:
                    traced_phone_numbers = client.skip_trace(
                        str(lead["ContactCity"]),
                        str(lead["ContactStreet"]),
                        str(lead["ContactState"]),
                        str(lead["ContactZip"]),
                        first_name=str(lead["ContactFirstName"]),
                        last_name=str(lead["ContactLastName"])
                    )

                    if traced_phone_numbers:
                        leads_lst = extend_and_add(leads_lst, skip_trace_result_col_num - 1, "TRUE")
                        try:
                            leads_lst = extend_and_add(leads_lst, contact_phone1_col_num - 1, traced_phone_numbers[0])
                            leads_lst = extend_and_add(leads_lst, contact_phone2_col_num - 1, traced_phone_numbers[1])
                            leads_lst = extend_and_add(leads_lst, contact_phone3_col_num - 1, traced_phone_numbers[2])
                        except IndexError:
                            pass
                    else:
                        logger.warning(f"No phone numbers found for address: {lead['ContactStreet']} {lead['ContactCity']} {lead['ContactState']} {lead['ContactZip']}")
                        leads_lst = extend_and_add(leads_lst, skip_trace_result_col_num - 1, "FALSE")
                except BatchAPIError as e:
                    logger.warning(e)
                    logger.info("Skip tracing skipped...")
                    break
                except Exception as e:
                    logger.error(e, exc_info=True)
                    leads_lst = extend_and_add(leads_lst, skip_trace_result_col_num - 1, "FALSE")
                skip_trace_count += 1
                sheet_client.sheet.update([leads_lst], f"A{row_num}")
            elif skip_trace_count >= 3:
                break


def validate_phones_lead(lead: dict):
    try:
        if (lead["SMS1QueuedDateTime"] == "" or lead["SMS2QueuedDateTime"] == "" or lead["SMS3QueuedDateTime"] == "") and lead["TargetStreet"]:
            return True
    except KeyError as e:
        logger.error(e, exc_info=True)

    return False


def not_queued_and_phone_present(lead, phone_key, queued_key):
    return lead[queued_key] == "" and lead[phone_key] != ""


def is_delay_met_for_phone(lead: dict, message_index: int, config: dict):
    if message_index > 1:
        previous_queued_key = f"SMS{message_index - 1}SentDateTime"
        if lead[previous_queued_key] != "":
            previous_queued_time = datetime.datetime.strptime(lead[previous_queued_key], "%m/%d/%Y %H:%M:%S")
            return previous_queued_time + datetime.timedelta(days=config["delay_between_messages"]) < datetime.datetime.now()
        else:
            return False
    return True


def queue_messages(sheet_client: GoogleSheetClient):
    with open(os.path.join(script_dir, 'config.json'), 'rb') as config_file:
        config = json.load(config_file)

    priority_mapping = config["types_mapping"]
    sheet_client.open_sheet("Message Templates")
    messages = sheet_client.read_records()
    messages = [message["Message"] for message in messages]

    sheet_client.open_sheet("Leads Master")
    leads = sheet_client.read_records()

    now = datetime.datetime.now()
    time_queued_str = now.strftime("%m/%d/%Y %H:%M:%S")
    msg_queued_col_numbers = {
        1: sheet_client.get_column_index("SMS1QueuedDateTime"),
        2: sheet_client.get_column_index("SMS2QueuedDateTime"),
        3: sheet_client.get_column_index("SMS3QueuedDateTime"),
    }
    leads_values = []
    logger.info("Running queue messages")

    for index, lead in enumerate(leads):
        row_num = index + 2
        leads_lst = [value for value in lead.values()]
        if validate_phones_lead(lead):
            for message_index in range(1, 4):
                phone_key = f"ContactPhone{message_index}"
                queued_key = f"SMS{message_index}QueuedDateTime"
                if not_queued_and_phone_present(lead, phone_key, queued_key) and is_delay_met_for_phone(lead, message_index, config):
                    try:
                        mapping = [mapping for mapping in priority_mapping.values() if mapping["display_name"] == lead["Type"]]
                        if mapping:
                            priority = mapping[0]["priority"]
                        else:
                            priority = 0
                            logger.warning(f"No mapping found with display name {lead['Type']}")
                    except KeyError:
                        priority = 0
                        logger.warning(f"Priority not found for \"{lead['Type']}\" lead type")
                    logger.info("Queuing sms")
                    message = random.choice(messages).replace("{TargetStreet}", lead["TargetStreet"])
                    payload = {
                        "recipient": f"+1{lead[phone_key]}",
                        "message": message,
                        "priority": priority
                    }
                    headers = {
                        'accept': 'application/json',
                        'Content-Type': 'application/json'
                    }
                    response = requests.post('http://localhost:4723/api/v1/sms/', headers=headers, json=payload)

                    if response.status_code == 201:
                        logger.info(response.content)
                        extend_and_add(leads_lst, msg_queued_col_numbers[message_index] - 1, time_queued_str)
                        lead[queued_key] = time_queued_str
                    else:
                        logger.error(f"Failed to queue message for lead {row_num}. Status code: {response.status_code}, Response: {response.text}")

        leads_values.append(leads_lst)

    sheet_client.sheet.update(leads_values, "A2")


def import_from_deal_machine(sheet_client: GoogleSheetClient, config: dict):
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

    existing_addresses = [" ".join([existing_lead["TargetStreet"].lower(), existing_lead["TargetCity"].lower(),
                                    existing_lead["TargetState"].lower(), str(existing_lead["TargetZip"])]) for
                          existing_lead in existing_leads]
    types_mapping = config["types_mapping"]
    now = datetime.datetime.now()
    values = []
    with DealMachineClient(logger, os.path.join(script_dir, "config.json")) as client:
        leads = client.get_leads()
        for index, lead in enumerate(leads):
            contact_address = lead["contact_address"]
            target_address = lead["target_address"]
            address = " ".join([target_address.street_name.lower(), target_address.city.lower(), target_address.state.lower(), target_address.zip])
            if address not in existing_addresses:
                try:
                    lead_type = types_mapping[lead["type"]]["display_name"]
                except KeyError:
                    lead_type = "Deal Machine Import"

                lead_values = []
                lead_values = extend_and_add(lead_values, target_street_col_num - 1, target_address.street_name)
                lead_values = extend_and_add(lead_values, target_city_col_num - 1, target_address.city)
                lead_values = extend_and_add(lead_values, target_state_col_num - 1, target_address.state)
                lead_values = extend_and_add(lead_values, target_zip_col_num - 1, target_address.zip)
                lead_values = extend_and_add(lead_values, contact_street_col_num - 1, contact_address.street_name)
                lead_values = extend_and_add(lead_values, contact_city_col_num - 1, contact_address.city)
                lead_values = extend_and_add(lead_values, contact_state_col_num - 1, contact_address.state)
                lead_values = extend_and_add(lead_values, contact_zip_col_num - 1, contact_address.zip)
                lead_values = extend_and_add(lead_values, datetime_added_col_num - 1, now.strftime("%m/%d/%Y %H:%M:%S"))
                lead_values = extend_and_add(lead_values, type_col_num - 1, lead_type)
                lead_values = extend_and_add(lead_values, source_col_num - 1, lead["creator"])
                existing_addresses.append(address)
                lead_values.pop(0)
                values.append(lead_values)
        values.append([""])
        sheet_client.sheet.update(values, f"B{last_row}")


if __name__ == "__main__":
    logger.info("TEST")
    with open(os.path.join(script_dir, 'config.json'), 'rb') as config_file:
        master_config = json.load(config_file)

    google_sheet_client = GoogleSheetClient(os.path.join(script_dir, "credentials-file.json"), "SAM", logger)

    import_from_deal_machine(google_sheet_client, master_config)
    # skip_trace(google_sheet_client)
    queue_messages(google_sheet_client)
