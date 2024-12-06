import os, platform
from typing import Optional

def select_int_in_range(msg: str, start: int, end: int) -> int:
    while True:
        val = input(msg)

        try:
            val = int(val)
        except ValueError:
            print(f"Invalid input. Please enter a number between {start} and {end}")
            continue

        if val < start or val > end:
            print(f"Invalid input. Please enter a number between {start} and {end}")
            continue

        return val
    
def select_int_in_range_with_abort(msg: str, start: int, end: int) -> Optional[int]:
    while True:
        val = input(msg)

        try:
            val = int(val)
        except ValueError:
            print(f"Invalid input. Please enter a number between {start} and {end}")
            continue

        if val == 0:
            return None

        if val < start or val > end:
            print(f"Invalid input. Please enter a number between {start} and {end}")
            continue

        return val


def try_again(msg: str) -> bool:
    try_again = input(f"{msg}. Would you like to try again? (Y/n): ")

    if try_again in "Yy":
        return True
    return False

def do_more() -> bool:
    return binary_decision("Is there anything else you would like to do today?")

def binary_decision(msg: str) -> bool:
    yes = input(f"{msg} (Y/n): ")
    if yes in "Yy":
        return True
    return False

def clear_stdout():
    if platform.system() == 'Windows':
        os.system('cls')
    else:
        os.system('clear')