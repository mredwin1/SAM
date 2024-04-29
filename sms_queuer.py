import datetime
import logging
import random

from clients import GoogleSheetClient

logger = logging.getLogger("sms_queuer_logger")
file_handler = logging.FileHandler("sms_queuer.log")
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(file_handler)
logger.setLevel(logging.INFO)


def validate_lead(lead: dict):
    try:
        if (lead["SMS1QueuedDateTime"] == "" or lead["SMS2QueuedDateTime"] == "" or lead["SMS3QueuedDateTime"] == "") and lead["TargetStreet"]:
            return True
    except KeyError as e:
        logger.error(e, exc_info=True)

    return False


def queue_message(sheet_client: GoogleSheetClient, row_num: int, message: str, recipient: str):
    now = datetime.datetime.now()
    time_sent_str = now.strftime("%m/%d/%Y %H:%M:%S")
    sheet_client.update_cell(row_num, 1, message)
    sheet_client.update_cell(row_num, 2, recipient)
    sheet_client.update_cell(row_num, 3, time_sent_str)

    return time_sent_str


def queue_messages(sheet_client: GoogleSheetClient):
    sheet_client.open_sheet("Message Templates")
    messages = sheet_client.read_records()
    messages = [message["Message"] for message in messages]

    sheet_client.open_sheet("Leads Master")

    leads = sheet_client.read_records()

    queue_message_sheet_client = GoogleSheetClient("credentials-file.json", "SAM")
    queue_message_sheet_client.open_sheet("Message Queue")
    queue_last_row = queue_message_sheet_client.get_last_row()

    for index, lead in enumerate(leads[:5]):
        row_num = index + 2
        if validate_lead(lead):
            for x in range(1, 2):
                if lead[f"SMS{x}QueuedDateTime"] == "" and lead[f"ContactPhone{x}"] != "":
                    message = random.choice(messages).replace("{TargetStreet}", lead["TargetStreet"])
                    time_sent = queue_message(queue_message_sheet_client, queue_last_row, message, lead[f"ContactPhone{x}"])
                    sheet_client.update_cell(row_num, sheet_client.get_column_index(f"SMS{x}QueuedDateTime"), time_sent)
                    queue_last_row += 1
        else:
            logger.warning(f"Lead not valid: {lead}")


if __name__ == "__main__":
    google_sheet_client = GoogleSheetClient("credentials-file.json", "SAM")
    queue_messages(google_sheet_client)
