# /Users/<you>/path/to/assignment02.py
"""
Course    : CSE 351
Assignment: 02
Student   : <your name here>
"""

# Don't import any other packages for this assignment
import os
import random
import threading
from money import *
from cse351 import *


def main():
    print('\nATM Processing Program:')
    print('=======================\n')

    create_data_files_if_needed()

    # Load ATM data files
    data_files = get_filenames('data_files')
    # debug: print(data_files)

    log = Log(show_terminal=True)
    log.start_timer()

    bank = Bank()

    # Start one ATM_Reader thread per data file
    readers = []
    for filepath in data_files:
        reader = ATM_Reader(filepath, bank)
        readers.append(reader)
        reader.start()

    # Wait for them to finish
    for r in readers:
        r.join()

    test_balances(bank)

    log.stop_timer('Total time')


# ===========================================================================
class ATM_Reader(threading.Thread):
    """Thread that reads all transactions from one ATM data file and applies them."""
    def __init__(self, filename: str, bank: 'Bank'):
        super().__init__()
        self.filename = filename
        self.bank = bank

    def run(self):
        count = 0
        print(f"[DEBUG] Starting reader for {self.filename}")
        try:
            with open(self.filename, 'r') as f:
                for line in f:
                    if line.startswith('#') or line.strip() == '':
                        continue
                    parts = line.strip().split(',')
                    if len(parts) != 3:
                        continue
                    account_num_s, trans_type, amount_str = parts
                    try:
                        account_num = int(account_num_s)
                    except ValueError:
                        continue

                    # Use Money constructed from the string (data files provide two-decimal strings).
                    # If your Money implementation expects a different constructor, adjust here.
                    try:
                        amount = Money(amount_str)
                    except Exception:
                        # Fallback: try converting to float then constructing Money
                        amount = Money(float(amount_str))

                    # Limited debug prints for the first few transactions in each file
                    if count < 5:
                        print(f"[DEBUG] File={os.path.basename(self.filename)}, acct={account_num}, "
                              f"type={trans_type}, raw={amount_str}, amount={amount}")
                        count += 1

                    if trans_type == 'd':
                        self.bank.deposit(account_num, amount)
                    elif trans_type == 'w':
                        self.bank.withdraw(account_num, amount)
        except FileNotFoundError:
            print(f"[ERROR] File not found: {self.filename}")


# ===========================================================================
class Account:
    """Thread-safe bank account using Money.add()/sub()."""
    def __init__(self):
        self.balance = Money('0')
        self.lock = threading.Lock()

    def deposit(self, amount: Money):
        # use Money.add() because Money does not support +=
        with self.lock:
            self.balance = self.balance.add(amount)

    def withdraw(self, amount: Money):
        # withdrawals allowed to go negative per assignment
        with self.lock:
            self.balance = self.balance.sub(amount)

    def get_balance(self) -> Money:
        with self.lock:
            return self.balance


# ===========================================================================
class Bank:
    """Manages numbered accounts (1..20)."""
    def __init__(self):
        self.accounts = {i: Account() for i in range(1, 21)}

    def deposit(self, account_number: int, amount: Money):
        # assume account_number is valid per assignment
        self.accounts[account_number].deposit(amount)

    def withdraw(self, account_number: int, amount: Money):
        self.accounts[account_number].withdraw(amount)

    def get_balance(self, account_number: int) -> Money:
        return self.accounts[account_number].get_balance()


# ---------------------------------------------------------------------------

def get_filenames(folder):
    """ Don't Change """
    filenames = []
    if not os.path.isdir(folder):
        return filenames
    for filename in os.listdir(folder):
        if filename.endswith(".dat"):
            filenames.append(os.path.join(folder, filename))
    # Keep deterministic ordering
    filenames.sort()
    return filenames


# ---------------------------------------------------------------------------
def create_data_files_if_needed():
    """ Don't Change """
    ATMS = 10
    ACCOUNTS = 20
    TRANSACTIONS = 250000

    sub_dir = 'data_files'
    if os.path.exists(sub_dir):
        return

    print('Creating Data Files: (Only runs once)')
    os.makedirs(sub_dir)

    random.seed(102030)
    mean = 100.00
    std_dev = 50.00

    for atm in range(1, ATMS + 1):
        filename = f'{sub_dir}/atm-{atm:02d}.dat'
        print(f'- {filename}')
        with open(filename, 'w') as f:
            f.write(f'# Atm transactions from machine {atm:02d}\n')
            f.write('# format: account number, type, amount\n')

            # create random transactions
            for i in range(TRANSACTIONS):
                account = random.randint(1, ACCOUNTS)
                trans_type = 'd' if random.randint(0, 1) == 0 else 'w'
                amount = f'{(random.gauss(mean, std_dev)):0.2f}'
                f.write(f'{account},{trans_type},{amount}\n')

    print()


# ---------------------------------------------------------------------------
def test_balances(bank):
    """ Don't Change """

    # Verify balances for each account
    correct_results = (
        (1, '59362.93'),
        (2, '11988.60'),
        (3, '35982.34'),
        (4, '-22474.29'),
        (5, '11998.99'),
        (6, '-42110.72'),
        (7, '-3038.78'),
        (8, '18118.83'),
        (9, '35529.50'),
        (10, '2722.01'),
        (11, '11194.88'),
        (12, '-37512.97'),
        (13, '-21252.47'),
        (14, '41287.06'),
        (15, '7766.52'),
        (16, '-26820.11'),
        (17, '15792.78'),
        (18, '-12626.83'),
        (19, '-59303.54'),
        (20, '-47460.38'),
    )

    wrong = False
    for account_number, balance in correct_results:
        bal = bank.get_balance(account_number)
        print(f'{account_number:02d}: balance = {bal}')
        if Money(balance) != bal:
            wrong = True
            print(f'Wrong Balance: account = {account_number}, expected = {balance}, actual = {bal}')

    if not wrong:
        print('\nAll account balances are correct')


if __name__ == "__main__":
    main()