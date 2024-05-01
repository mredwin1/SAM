import datetime
import json
import logging
import os
import random

from clients import GoogleSheetClient, BatchDataClient

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
    values = []
    with BatchDataClient(logger, os.path.join(script_dir, "config.json")) as client:
        for index, lead in enumerate(leads):
            leads_lst = [value for value in lead.values()]
            if validate_lead(lead):
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
                        extend_and_add(leads_lst, skip_trace_result_col_num - 1, "TRUE")
                        try:
                            extend_and_add(leads_lst, contact_phone1_col_num - 1, traced_phone_numbers[0])
                            extend_and_add(leads_lst, contact_phone2_col_num - 1, traced_phone_numbers[1])
                            extend_and_add(leads_lst, contact_phone3_col_num - 1, traced_phone_numbers[2])
                        except IndexError:
                            pass
                    else:
                        logger.warning(f"No phone numbers found for lead: {lead}")
                        extend_and_add(leads_lst, skip_trace_result_col_num - 1, "FALSE")
                except Exception as e:
                    logger.error(e, exc_info=True)
                    extend_and_add(leads_lst, skip_trace_result_col_num - 1, "FALSE")
            values.append(leads_lst)
    sheet_client.sheet.update(values, "A2")


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
        previous_queued_key = f"SMS{message_index - 1}QueuedDateTime"
        if lead[previous_queued_key] != "":
            previous_queued_time = datetime.datetime.strptime(lead[previous_queued_key], "%m/%d/%Y %H:%M:%S")
            return previous_queued_time + datetime.timedelta(days=config["delay_between_messages"]) < datetime.datetime.now()
        else:
            return False
    return True


def queue_messages(sheet_client: GoogleSheetClient):
    with open(os.path.join(script_dir, 'config.json'), 'rb') as config_file:
        config = json.load(config_file)

    sheet_client.open_sheet("Message Templates")
    messages = sheet_client.read_records()
    messages = [message["Message"] for message in messages]

    sheet_client.open_sheet("Leads Master")

    leads = sheet_client.read_records()

    queue_message_sheet_client = GoogleSheetClient(os.path.join(script_dir, "credentials-file.json"), "SAM", logger)
    queue_message_sheet_client.open_sheet("Message Queue")
    queue_last_row = queue_message_sheet_client.get_last_row()
    message_col_num = queue_message_sheet_client.get_column_index("Message")
    recipient_col_num = queue_message_sheet_client.get_column_index("Recipient")
    time_queued_col_num = queue_message_sheet_client.get_column_index("DateTimeQueued")
    now = datetime.datetime.now()
    time_queued_str = now.strftime("%m/%d/%Y %H:%M:%S")
    msg_queued_col_numbers = {
        1: sheet_client.get_column_index("SMS1QueuedDateTime"),
        2: sheet_client.get_column_index("SMS2QueuedDateTime"),
        3: sheet_client.get_column_index("SMS3QueuedDateTime"),
    }
    leads_values = []
    message_queue_value = []
    for index, lead in enumerate(leads):
        leads_lst = [value for value in lead.values()]
        if validate_phones_lead(lead):
            for message_index in range(1, 4):
                phone_key = f"ContactPhone{message_index}"
                queued_key = f"SMS{message_index}QueuedDateTime"
                if not_queued_and_phone_present(lead, phone_key, queued_key) and is_delay_met_for_phone(lead, message_index, config):
                    message_queue_lst = []
                    message = random.choice(messages).replace("{TargetStreet}", lead["TargetStreet"])
                    extend_and_add(message_queue_lst, message_col_num - 1, message)
                    extend_and_add(message_queue_lst, recipient_col_num - 1, lead[phone_key])
                    extend_and_add(message_queue_lst, time_queued_col_num - 1, time_queued_str)
                    extend_and_add(leads_lst, msg_queued_col_numbers[message_index] - 1, time_queued_str)

                    lead[queued_key] = time_queued_str
                    message_queue_value.append(message_queue_lst)

        leads_values.append(leads_lst)
    sheet_client.sheet.update(leads_values, "A2")
    queue_message_sheet_client.sheet.update(message_queue_value, f"A{queue_last_row}")


if __name__ == "__main__":
    with open(os.path.join(script_dir, 'config.json'), 'rb') as config_file:
        master_config = json.load(config_file)

    google_sheet_client = GoogleSheetClient(os.path.join(script_dir, "credentials-file.json"), "SAM", logger)

    skip_trace(google_sheet_client)
    queue_messages(google_sheet_client)
