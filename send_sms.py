import datetime
import json
import logging
import os
import random
import time

from clients import GoogleSheetClient, HushedClient

script_dir = os.path.dirname(os.path.abspath(__file__))
logger = logging.getLogger("send_sms_logger")
file_handler = logging.FileHandler("send_sms.log")
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


def calculate_msgs_to_send(msgs_left: int, send_prob: int, interval=3, current_time: datetime.datetime = None):
    if not current_time:
        current_time = datetime.datetime.now()

    # Calculate the number of minutes left in the day
    mins_left = (60 - current_time.minute)

    # Calculate the number of intervals left in the day
    intervals_left = max(int(mins_left) // interval, 1)
    max_per_interval = 4

    msgs_to_send = msgs_left // intervals_left

    if intervals_left > 3 and random.random() < .5:
        msgs_to_send = len([i for i in range(msgs_to_send) if random.randint(0, 100) >= send_prob])

    return min(msgs_to_send, max_per_interval)


def get_latest_phone_number(messages):
    """
    This function takes a list of dictionaries where each dictionary has a 'Message', 'DateTimeSent', and 'SenderNumber'.
    It returns the phone number of the sender from the latest message.
    """
    def parse_date(date_str):
        try:
            return datetime.datetime.strptime(date_str, '%m/%d/%Y %H:%M:%S')
        except ValueError:
            return datetime.datetime.min  # Return a minimal datetime for blank or incorrect formats

    # Sorting the list of dictionaries by 'DateTimeSent' after converting to datetime objects
    messages_sorted = sorted(messages, key=lambda x: parse_date(x['DateTimeSent']) if x['DateTimeSent'] else datetime.datetime.min)

    # Getting the last element from the sorted list to find the latest message
    latest_message = messages_sorted[-1]

    # Extracting and returning the 'SenderNumber' from the latest message
    return latest_message['SenderNumber']


def send_messages(sheet_client: GoogleSheetClient, config: dict):
    sheet_client.open_sheet("Message Queue")
    messages = sheet_client.read_records()
    time_sent_column_number = sheet_client.get_column_index("DateTimeSent")
    sender_number_column_number = sheet_client.get_column_index("SenderNumber")
    messages_to_send = []
    execution_time = datetime.datetime.now()

    numbers = {number.replace("+", ""): 0 for number in config["numbers_for_send"]}

    for index, message in enumerate(messages):
        row_num = index + 2
        datetime_sent_str = message["DateTimeSent"]
        datetime_queued_str = message["DateTimeQueued"]
        message_str = str(message["Message"])
        recipient_str = str(message["Recipient"])
        sender_number = str(message["SenderNumber"])
        lead_row_num = int(message["LeadRowNum"]) if message["LeadRowNum"] else 0
        number_index = int(message["PhoneNumIndex"]) if message["PhoneNumIndex"] else 0
        priority = int(message["Priority"])
        try:
            if not datetime_sent_str and message_str and recipient_str:
                messages_to_send.append({
                    "index": row_num - 2,
                    "recipient": recipient_str,
                    "message": message_str,
                    "lead_row_num": lead_row_num,
                    "number_index": number_index,
                    "priority": priority,
                    "datetime_queued": datetime_queued_str
                })
            elif message["DateTimeSent"] and sender_number:
                try:
                    datetime_sent = datetime.datetime.strptime(datetime_sent_str, "%m/%d/%Y %H:%M:%S")
                    if datetime_sent.year == execution_time.year and datetime_sent.month == execution_time.month and datetime_sent.day == execution_time.day and datetime_sent.hour == execution_time.hour:
                        numbers[sender_number] += 1
                except KeyError as e:
                    logger.warning(e, exc_info=True)
                except ValueError:
                    logger.error(f"Could not parse DateTimeSent in row number {row_num}")
        except KeyError:
            logger.warning(f"Skipping processing row {row_num} due to missing columns")

    if messages_to_send:
        messages_to_send = sorted(
            messages_to_send,
            key=lambda i: (-i['priority'], datetime.datetime.strptime(i['datetime_queued'], '%m/%d/%Y %H:%M:%S'))
        )

        logger.info(f"Message counts: {numbers}")
        queued_messages = [[value for value in queued_message.values()] for queued_message in messages]
        available_numbers = [key for key, value in numbers.items() if value < config["messages_per_hour"]]
        if available_numbers:
            run_interval = config["leads_manager_run_interval"]
            max_per_hour = config["messages_per_hour"] * len(config["numbers_for_send"])
            chance_to_send_messages = config["chance_to_send"]
            num_messages_to_send = calculate_msgs_to_send(
                max_per_hour - sum(numbers.values()),
                chance_to_send_messages,
                run_interval
            )
            logger.info(f"{len(messages_to_send)} messages in queue and chose to send {num_messages_to_send} right now")
            if num_messages_to_send:
                leads_master_sheet_client = GoogleSheetClient(os.path.join(script_dir, "credentials-file.json"), "SAM", logger)
                leads_master_sheet_client.open_sheet("Leads Master")
                msg_queued_col_numbers = {
                    1: leads_master_sheet_client.get_column_index("SMS1SentDateTime"),
                    2: leads_master_sheet_client.get_column_index("SMS2SentDateTime"),
                    3: leads_master_sheet_client.get_column_index("SMS3SentDateTime"),
                }

                last_number = get_latest_phone_number(messages)
                with HushedClient(config["phone_uuid"], logger, config["appium_url"]) as client:
                    for x in range(num_messages_to_send):
                        logger.info(f"=========================================================")
                        logger.info(f"Latest number used for sending: {last_number}")
                        logger.info(f"Available numbers: {available_numbers}")
                        try:
                            message_to_send = messages_to_send.pop(0)

                            if len(available_numbers) > 1:
                                number_for_sending = random.choice([number for number in available_numbers if number != last_number])
                            else:
                                number_for_sending = available_numbers[0]

                            logger.info(f"Sending \"{message_to_send['message']}\" to {message_to_send['recipient']} from {number_for_sending}")

                            client.send_sms(f"+{number_for_sending}", message_to_send["recipient"], message_to_send["message"])
                            time_sent = datetime.datetime.now()

                            queued_messages[message_to_send["index"]] = extend_and_add(queued_messages[message_to_send["index"]], time_sent_column_number - 1, time_sent.strftime("%m/%d/%Y %H:%M:%S"))
                            queued_messages[message_to_send["index"]] = extend_and_add(queued_messages[message_to_send["index"]], sender_number_column_number - 1, number_for_sending)
                            if message_to_send["lead_row_num"]:
                                row = message_to_send["lead_row_num"]
                                col = msg_queued_col_numbers[message_to_send["number_index"]]
                                logger.info(f"Sent datetime written to Leads Master - Row: {row} Col: {col}")
                                leads_master_sheet_client.sheet.update_cell(row, col, time_sent.strftime("%m/%d/%Y %H:%M:%S"))
                            logger.info(f"Sent \"{message_to_send['message']}\" to {message_to_send['recipient']} from {number_for_sending}")

                            numbers[number_for_sending] += 1
                            available_numbers = [key for key, value in numbers.items() if value < config["messages_per_hour"]]
                            last_number = number_for_sending
                            time.sleep(random.randint(25, 35))
                        except IndexError as e:
                            logger.error(e, exc_info=True)
                        logger.info(f"=========================================================")
                        sheet_client.sheet.update(queued_messages, "A2")
    else:
        logger.info("No messages to send in queue")


if __name__ == "__main__":
    with open(os.path.join(script_dir, 'config.json'), 'rb') as config_file:
        master_config = json.load(config_file)

    google_sheet_client = GoogleSheetClient(os.path.join(script_dir, "credentials-file.json"), "SAM", logger)

    send_messages(google_sheet_client, master_config)
